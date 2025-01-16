package cmdline

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

func init() {
	var useID bool
	cmd := cobra.Command{
		Use:   "load",
		Short: "Load data from CDS to a storage service",
		Args:  cobra.ExactArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			cmdCtx := GetCmdCtx(cmd)

			if useID {
				pkgID, err := strconv.ParseInt(args[0], 10, 64)
				if err != nil {
					fmt.Printf("Invalid package ID: %s\n", args[0])
				}

				stgID, err := strconv.ParseInt(args[1], 10, 64)
				if err != nil {
					fmt.Printf("Invalid storage ID: %s\n", args[1])
				}

				loadByID(cmdCtx, cdssdk.PackageID(pkgID), cdssdk.StorageID(stgID), args[2])
			} else {
				loadByPath(cmdCtx, args[0], args[1], args[2])
			}
		},
	}
	cmd.Flags().BoolVarP(&useID, "id", "i", false, "Use ID for both package and storage service instead of their name or path")
	rootCmd.AddCommand(&cmd)
}

func loadByPath(cmdCtx *CommandContext, pkgPath string, stgName string, rootPath string) {
	userID := cdssdk.UserID(1)

	comps := strings.Split(strings.Trim(pkgPath, cdssdk.ObjectPathSeparator), cdssdk.ObjectPathSeparator)
	if len(comps) != 2 {
		fmt.Printf("Package path must be in format of <bucket>/<package>")
		return
	}

	pkg, err := cmdCtx.Cmdline.Svc.PackageSvc().GetByFullName(userID, comps[0], comps[1])
	if err != nil {
		fmt.Println(err)
		return
	}

	stg, err := cmdCtx.Cmdline.Svc.StorageSvc().GetByName(userID, stgName)
	if err != nil {
		fmt.Println(err)
		return
	}

	loadByID(cmdCtx, pkg.PackageID, stg.StorageID, rootPath)
}

func loadByID(cmdCtx *CommandContext, pkgID cdssdk.PackageID, stgID cdssdk.StorageID, rootPath string) {
	userID := cdssdk.UserID(1)
	startTime := time.Now()

	err := cmdCtx.Cmdline.Svc.StorageSvc().LoadPackage(userID, pkgID, stgID, rootPath)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("Package loaded to: %v:%v in %v\n", stgID, rootPath, time.Since(startTime))
}
