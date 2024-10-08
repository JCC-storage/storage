//go:build mage

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"gitlink.org.cn/cloudream/common/magefiles"

	//mage:import
	_ "gitlink.org.cn/cloudream/common/magefiles/targets"

	cp "github.com/otiai10/copy"
)

const (
	BuildDir = "./build"
)

func All() error {
	if err := Bin(); err != nil {
		return err
	}

	if err := Scripts(); err != nil {
		return err
	}

	if err := Confs(); err != nil {
		return err
	}

	return nil
}

func Bin() error {
	if err := Agent(); err != nil {
		return err
	}
	if err := Client(); err != nil {
		return err
	}
	if err := Coordinator(); err != nil {
		return err
	}
	if err := Scanner(); err != nil {
		return err
	}

	return nil
}

func Scripts() error {
	scriptsDir := "./common/assets/scripts"

	info, err := os.Stat(scriptsDir)
	if errors.Is(err, os.ErrNotExist) {
		fmt.Printf("no scripts.\n")
		return nil
	}

	if !info.IsDir() {
		return fmt.Errorf("scripts is not a directory")
	}

	fullDirPath, err := filepath.Abs(filepath.Join(BuildDir, "scripts"))
	if err != nil {
		return err
	}

	fmt.Printf("copying scripts to %s\n", fullDirPath)

	return cp.Copy(scriptsDir, fullDirPath)
}

func Confs() error {
	confDir := "./common/assets/confs"

	info, err := os.Stat(confDir)
	if errors.Is(err, os.ErrNotExist) {
		fmt.Printf("no confs.\n")
		return nil
	}

	if !info.IsDir() {
		return fmt.Errorf("confs is not a directory")
	}

	fullDirPath, err := filepath.Abs(filepath.Join(BuildDir, "confs"))
	if err != nil {
		return err
	}

	fmt.Printf("copying confs to %s\n", fullDirPath)

	return cp.Copy(confDir, fullDirPath)
}

func Agent() error {
	return magefiles.Build(magefiles.BuildArgs{
		OutputName: "agent",
		OutputDir:  "agent",
		AssetsDir:  "assets",
		EntryFile:  "agent/main.go",
	})
}

func Client() error {
	return magefiles.Build(magefiles.BuildArgs{
		OutputName: "client",
		OutputDir:  "client",
		AssetsDir:  "assets",
		EntryFile:  "client/main.go",
	})
}

func Coordinator() error {
	return magefiles.Build(magefiles.BuildArgs{
		OutputName: "coordinator",
		OutputDir:  "coordinator",
		AssetsDir:  "assets",
		EntryFile:  "coordinator/main.go",
	})
}

func Scanner() error {
	return magefiles.Build(magefiles.BuildArgs{
		OutputName: "scanner",
		OutputDir:  "scanner",
		AssetsDir:  "assets",
		EntryFile:  "scanner/main.go",
	})
}
