package utils

import (
	"path/filepath"
)

func MakeStorageLoadDirectory(stgDir string) string {
	return filepath.Join(stgDir, "packages")
}
