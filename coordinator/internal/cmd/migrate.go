package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db2/model"
	"gitlink.org.cn/cloudream/storage/coordinator/internal/config"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func init() {
	var configPath string
	cmd := cobra.Command{
		Use:   "migrate",
		Short: "Run database migrations",
		Run: func(cmd *cobra.Command, args []string) {
			migrate(configPath)
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to config file")
	RootCmd.AddCommand(&cmd)
}

func migrate(configPath string) {
	// TODO 将create_database.sql的内容逐渐移动到这里来

	err := config.Init(configPath)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	db, err := gorm.Open(mysql.Open(config.Cfg().DB.MakeSourceString()))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	migrateOne(db, cdssdk.Bucket{})
	migrateOne(db, model.Cache{})
	migrateOne(db, model.Location{})
	migrateOne(db, model.HubConnectivity{})
	migrateOne(db, cdssdk.Hub{})
	migrateOne(db, stgmod.ObjectAccessStat{})
	migrateOne(db, stgmod.ObjectBlock{})
	migrateOne(db, cdssdk.Object{})
	migrateOne(db, stgmod.PackageAccessStat{})
	migrateOne(db, cdssdk.Package{})
	migrateOne(db, cdssdk.PinnedObject{})
	migrateOne(db, cdssdk.Storage{})
	migrateOne(db, model.UserStorage{})
	migrateOne(db, model.UserBucket{})
	migrateOne(db, model.User{})
	migrateOne(db, model.UserHub{})

	fmt.Println("migrate success")
}

func migrateOne[T any](db *gorm.DB, model T) {
	err := db.AutoMigrate(model)
	if err != nil {
		fmt.Printf("migratting model %T: %v\n", model, err)
		os.Exit(1)
	}
}
