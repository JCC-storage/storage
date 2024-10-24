package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
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

	err = db.AutoMigrate(&cdssdk.Node{})
	if err != nil {
		fmt.Printf("migratting model Node: %v\n", err)
		os.Exit(1)
	}

	err = db.AutoMigrate(&cdssdk.Storage{})
	if err != nil {
		fmt.Printf("migratting model Storage: %v\n", err)
		os.Exit(1)
	}

	err = db.AutoMigrate(&cdssdk.ShardStorage{})
	if err != nil {
		fmt.Printf("migratting model ShardStorage: %v\n", err)
		os.Exit(1)
	}

	err = db.AutoMigrate(&cdssdk.SharedStorage{})
	if err != nil {
		fmt.Printf("migratting model SharedStorage: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("migrate success")
}
