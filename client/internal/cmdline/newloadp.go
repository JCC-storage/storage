package cmdline

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/spf13/cobra"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

func init() {
	cmd := &cobra.Command{
		Use:   "newloadp localPath bucketID packageName storageID...",
		Short: "Create a new package then upload an load files to it at the same time",
		Args:  cobra.MinimumNArgs(4),
		Run: func(cmd *cobra.Command, args []string) {
			cmdCtx := GetCmdCtx(cmd)
			localPath := args[0]

			bktID, err := strconv.ParseInt(args[1], 10, 64)
			if err != nil {
				fmt.Println(err)
				return
			}

			packageName := args[2]
			storageIDs := make([]cdssdk.StorageID, 0)
			for _, sID := range args[3:] {
				sID, err := strconv.ParseInt(sID, 10, 64)
				if err != nil {
					fmt.Println(err)
					return
				}
				storageIDs = append(storageIDs, cdssdk.StorageID(sID))
			}

			newloadp(cmdCtx, localPath, cdssdk.BucketID(bktID), packageName, storageIDs)
		},
	}

	rootCmd.AddCommand(cmd)
}

func newloadp(cmdCtx *CommandContext, path string, bucketID cdssdk.BucketID, packageName string, storageIDs []cdssdk.StorageID) {
	userID := cdssdk.UserID(1)

	up, err := cmdCtx.Cmdline.Svc.Uploader.BeginCreateLoad(userID, bucketID, packageName, storageIDs)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer up.Abort()

	var fileCount int
	var totalSize int64
	err = filepath.WalkDir(path, func(fname string, fi os.DirEntry, err error) error {
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

	ret, err := up.Commit()
	if err != nil {
		fmt.Printf("committing package: %v\n", err)
		return
	}

	wr := table.NewWriter()
	wr.AppendHeader(table.Row{"ID", "Name", "FileCount", "TotalSize", "LoadedDirs"})
	wr.AppendRow(table.Row{ret.Package.PackageID, ret.Package.Name, fileCount, totalSize, strings.Join(ret.LoadedDirs, "\n")})
	fmt.Println(wr.Render())
}