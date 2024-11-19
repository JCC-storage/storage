package cmdline

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/inhies/go-bytesize"
	"github.com/spf13/cobra"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

func init() {
	var stgID int64
	cmd := &cobra.Command{
		Use:   "put [local] [remote]",
		Short: "Upload files to CDS",
		Args: func(cmd *cobra.Command, args []string) error {
			if err := cobra.ExactArgs(2)(cmd, args); err != nil {
				return err
			}

			remote := args[1]
			comps := strings.Split(strings.Trim(remote, cdssdk.ObjectPathSeparator), cdssdk.ObjectPathSeparator)
			if len(comps) != 2 {
				return fmt.Errorf("invalid remote path: %s, which must be in format of <bucket>/<package>", remote)
			}

			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			userID := cdssdk.UserID(1)
			cmdCtx := GetCmdCtx(cmd)

			local := args[0]
			remote := args[1]
			comps := strings.Split(strings.Trim(remote, cdssdk.ObjectPathSeparator), cdssdk.ObjectPathSeparator)

			startTime := time.Now()

			bkt, err := cmdCtx.Cmdline.Svc.BucketSvc().GetBucketByName(userID, comps[0])
			if err != nil {
				fmt.Printf("getting bucket: %v\n", err)
				return
			}

			pkg, err := cmdCtx.Cmdline.Svc.PackageSvc().GetByName(userID, comps[0], comps[1])
			if err != nil {
				if codeMsg, ok := err.(*mq.CodeMessageError); ok && codeMsg.Code == errorcode.DataNotFound {
					pkg2, err := cmdCtx.Cmdline.Svc.PackageSvc().Create(userID, bkt.BucketID, comps[1])
					if err != nil {
						fmt.Printf("creating package: %v\n", err)
						return
					}
					pkg = &pkg2

				} else {
					fmt.Printf("getting package: %v\n", err)
					return
				}
			}
			var storageAff cdssdk.StorageID
			if stgID != 0 {
				storageAff = cdssdk.StorageID(stgID)
			}

			up, err := cmdCtx.Cmdline.Svc.Uploader.BeginUpdate(userID, pkg.PackageID, storageAff)
			if err != nil {
				fmt.Printf("begin updating package: %v\n", err)
				return
			}
			defer up.Abort()

			var fileCount int
			var totalSize int64
			err = filepath.WalkDir(local, func(fname string, fi os.DirEntry, err error) error {
				if err != nil {
					return nil
				}

				if fi.IsDir() {
					return nil
				}

				fileCount++

				info, err := fi.Info()
				if err != nil {
					return err
				}
				totalSize += info.Size()

				file, err := os.Open(fname)
				if err != nil {
					return err
				}
				defer file.Close()

				return up.Upload(fname, info.Size(), file)
			})
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			_, err = up.Commit()
			if err != nil {
				fmt.Printf("committing package: %v\n", err)
				return
			}

			fmt.Printf("Put %v files (%v) to %s in %v.\n", fileCount, bytesize.ByteSize(totalSize), remote, time.Since(startTime))
		},
	}
	cmd.Flags().Int64VarP(&stgID, "storage", "s", 0, "storage affinity")

	rootCmd.AddCommand(cmd)
}
