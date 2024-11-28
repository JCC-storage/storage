package local

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/utils"
)

type SharedStore struct {
	svc *Service
	cfg cdssdk.LocalSharedStorage
	// lock sync.Mutex
}

func NewSharedStore(svc *Service, cfg cdssdk.LocalSharedStorage) (*SharedStore, error) {
	return &SharedStore{
		svc: svc,
		cfg: cfg,
	}, nil
}

func (s *SharedStore) Start(ch *types.StorageEventChan) {
	s.getLogger().Infof("component start, LoadBase: %v", s.cfg.LoadBase)
}

func (s *SharedStore) Stop() {
	s.getLogger().Infof("component stop")
}

func (s *SharedStore) WritePackageObject(userID cdssdk.UserID, pkgID cdssdk.PackageID, path string, stream io.Reader) (string, error) {
	relaPath := filepath.Join(utils.MakeLoadedPackagePath(userID, pkgID), path)
	fullPath := filepath.Join(s.cfg.LoadBase, relaPath)
	err := os.MkdirAll(filepath.Dir(fullPath), 0755)
	if err != nil {
		return "", err
	}

	f, err := os.Create(fullPath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	_, err = io.Copy(f, stream)
	if err != nil {
		return "", err
	}

	return filepath.ToSlash(relaPath), nil
}

func (s *SharedStore) ListLoadedPackages() ([]stgmod.LoadedPackageID, error) {
	entries, err := os.ReadDir(s.cfg.LoadBase)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		s.getLogger().Warnf("list package directory: %v", err)
		return nil, err
	}

	var loadeds []stgmod.LoadedPackageID
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}

		uid, err := strconv.ParseInt(e.Name(), 10, 64)
		if err != nil {
			continue
		}

		userID := cdssdk.UserID(uid)
		pkgs, err := s.listUserPackages(userID, fmt.Sprintf("%v", userID))
		if err != nil {
			continue
		}

		loadeds = append(loadeds, pkgs...)
	}

	return loadeds, nil
}

func (s *SharedStore) listUserPackages(userID cdssdk.UserID, userIDStr string) ([]stgmod.LoadedPackageID, error) {
	userDir := filepath.Join(s.cfg.LoadBase, userIDStr)
	entries, err := os.ReadDir(userDir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		s.getLogger().Warnf("list package directory: %v", err)
		return nil, err
	}

	var pkgs []stgmod.LoadedPackageID
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}

		pkgID, err := strconv.ParseInt(e.Name(), 10, 64)
		if err != nil {
			continue
		}

		pkgs = append(pkgs, stgmod.LoadedPackageID{
			UserID:    userID,
			PackageID: cdssdk.PackageID(pkgID),
		})
	}

	return pkgs, nil
}

func (s *SharedStore) PackageGC(avaiables []stgmod.LoadedPackageID) error {
	log := s.getLogger()

	entries, err := os.ReadDir(s.cfg.LoadBase)
	if err != nil {
		log.Warnf("list storage directory: %s", err.Error())
		return err
	}

	// userID->pkgID->pkg
	userPkgs := make(map[string]map[string]bool)
	for _, pkg := range avaiables {
		userIDStr := fmt.Sprintf("%v", pkg.UserID)

		pkgs, ok := userPkgs[userIDStr]
		if !ok {
			pkgs = make(map[string]bool)
			userPkgs[userIDStr] = pkgs
		}

		pkgIDStr := fmt.Sprintf("%v", pkg.PackageID)
		pkgs[pkgIDStr] = true
	}

	userDirs := lo.Filter(entries, func(info fs.DirEntry, index int) bool { return info.IsDir() })
	for _, dir := range userDirs {
		pkgMap, ok := userPkgs[dir.Name()]
		// 第一级目录名是UserID，先删除UserID在StoragePackage表里没出现过的文件夹
		if !ok {
			rmPath := filepath.Join(s.cfg.LoadBase, dir.Name())
			err := os.RemoveAll(rmPath)
			if err != nil {
				log.Warnf("removing user dir %s: %s", rmPath, err.Error())
			} else {
				log.Debugf("user dir %s removed by gc", rmPath)
			}
			continue
		}

		pkgDir := filepath.Join(s.cfg.LoadBase, dir.Name())
		// 遍历每个UserID目录的packages目录里的内容
		pkgs, err := os.ReadDir(pkgDir)
		if err != nil {
			log.Warnf("reading package dir %s: %s", pkgDir, err.Error())
			continue
		}

		for _, pkg := range pkgs {
			if !pkgMap[pkg.Name()] {
				rmPath := filepath.Join(pkgDir, pkg.Name())
				err := os.RemoveAll(rmPath)
				if err != nil {
					log.Warnf("removing package dir %s: %s", rmPath, err.Error())
				} else {
					log.Debugf("package dir %s removed by gc", rmPath)
				}
			}
		}
	}

	return nil
}

func (s *SharedStore) getLogger() logger.Logger {
	return logger.WithField("SharedStore", "Local").WithField("Storage", s.svc.Detail.Storage.String())
}

type PackageWriter struct {
	pkgRoot     string
	fullDirPath string
}

func (w *PackageWriter) Root() string {
	return w.pkgRoot
}

func (w *PackageWriter) Write(path string, stream io.Reader) (string, error) {
	fullFilePath := filepath.Join(w.fullDirPath, path)
	err := os.MkdirAll(filepath.Dir(fullFilePath), 0755)
	if err != nil {
		return "", err
	}

	f, err := os.Create(fullFilePath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	_, err = io.Copy(f, stream)
	if err != nil {
		return "", err
	}

	return filepath.ToSlash(filepath.Join(w.pkgRoot, path)), nil
}
