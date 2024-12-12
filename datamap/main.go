package main

import (
	"fmt"
	"os"

	"gitlink.org.cn/cloudream/storage/datamap/internal/config"
)

func main() {
	err := config.Init()
	if err != nil {
		fmt.Printf("init config failed, err: %s", err.Error())
		os.Exit(1)
	}
}
