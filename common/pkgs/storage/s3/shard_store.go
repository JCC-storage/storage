package s3

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/common/utils/os2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/storage/types"
)

const (
	TempDir   = "tmp"
	BlocksDir = "blocks"
)

type ShardStoreOption struct {
	UseAWSSha256 bool // 能否直接使用AWS提供的SHA256校验，如果不行，则使用本地计算。默认使用本地计算。
}

type ShardStore struct {
	svc              *Service
	cli              *s3.Client
	bucket           string
	cfg              cdssdk.S3ShardStorage
	opt              ShardStoreOption
	lock             sync.Mutex
	workingTempFiles map[string]bool
	done             chan any
}

func NewShardStore(svc *Service, cli *s3.Client, bkt string, cfg cdssdk.S3ShardStorage, opt ShardStoreOption) (*ShardStore, error) {
	return &ShardStore{
		svc:              svc,
		cli:              cli,
		bucket:           bkt,
		cfg:              cfg,
		opt:              opt,
		workingTempFiles: make(map[string]bool),
		done:             make(chan any, 1),
	}, nil
}

func (s *ShardStore) Start(ch *types.StorageEventChan) {
	s.getLogger().Infof("component start, root: %v", s.cfg.Root)

	go func() {
		removeTempTicker := time.NewTicker(time.Minute * 10)
		defer removeTempTicker.Stop()

		for {
			select {
			case <-removeTempTicker.C:
				s.removeUnusedTempFiles()
			case <-s.done:
				return
			}
		}
	}()
}

func (s *ShardStore) removeUnusedTempFiles() {
	s.lock.Lock()
	defer s.lock.Unlock()

	log := s.getLogger()

	var deletes []s3types.ObjectIdentifier
	deleteObjs := make(map[string]s3types.Object)
	var marker *string
	for {
		resp, err := s.cli.ListObjects(context.Background(), &s3.ListObjectsInput{
			Bucket: aws.String(s.bucket),
			Prefix: aws.String(JoinKey(s.cfg.Root, TempDir, "/")),
			Marker: marker,
		})

		if err != nil {
			log.Warnf("read temp dir: %v", err)
			return
		}

		for _, obj := range resp.Contents {
			objName := BaseKey(*obj.Key)

			if s.workingTempFiles[objName] {
				continue
			}

			deletes = append(deletes, s3types.ObjectIdentifier{
				Key: obj.Key,
			})
			deleteObjs[*obj.Key] = obj
		}

		if !*resp.IsTruncated {
			break
		}

		marker = resp.NextMarker
	}

	if len(deletes) == 0 {
		return
	}

	resp, err := s.cli.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
		Bucket: aws.String(s.bucket),
		Delete: &s3types.Delete{
			Objects: deletes,
		},
	})
	if err != nil {
		log.Warnf("delete temp files: %v", err)
		return
	}

	for _, del := range resp.Deleted {
		obj := deleteObjs[*del.Key]
		log.Infof("remove unused temp file %v, size: %v, last mod time: %v", *obj.Key, *obj.Size, *obj.LastModified)
	}
}

func (s *ShardStore) Stop() {
	s.getLogger().Infof("component stop")

	select {
	case s.done <- nil:
	default:
	}
}

func (s *ShardStore) Create(stream io.Reader) (types.FileInfo, error) {
	if s.opt.UseAWSSha256 {
		return s.createWithAwsSha256(stream)
	} else {
		return s.createWithCalcSha256(stream)
	}
}

func (s *ShardStore) createWithAwsSha256(stream io.Reader) (types.FileInfo, error) {
	log := s.getLogger()

	key, fileName := s.createTempFile()

	counter := io2.NewCounter(stream)

	resp, err := s.cli.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:            aws.String(s.bucket),
		Key:               aws.String(key),
		Body:              counter,
		ChecksumAlgorithm: s3types.ChecksumAlgorithmSha256,
	})
	if err != nil {
		log.Warnf("uploading file %v: %v", key, err)

		s.lock.Lock()
		defer s.lock.Unlock()

		delete(s.workingTempFiles, fileName)
		return types.FileInfo{}, err
	}

	if resp.ChecksumSHA256 == nil {
		log.Warnf("SHA256 checksum not found in response of uploaded file %v", key)
		s.onCreateFailed(key, fileName)
		return types.FileInfo{}, errors.New("SHA256 checksum not found in response")
	}

	hash, err := DecodeBase64Hash(*resp.ChecksumSHA256)
	if err != nil {
		log.Warnf("decode SHA256 checksum %v: %v", *resp.ChecksumSHA256, err)
		s.onCreateFailed(key, fileName)
		return types.FileInfo{}, fmt.Errorf("decode SHA256 checksum: %v", err)
	}

	return s.onCreateFinished(key, counter.Count(), cdssdk.NewFullHash(hash))
}

func (s *ShardStore) createWithCalcSha256(stream io.Reader) (types.FileInfo, error) {
	log := s.getLogger()

	key, fileName := s.createTempFile()

	hashStr := io2.NewReadHasher(sha256.New(), stream)
	counter := io2.NewCounter(hashStr)

	_, err := s.cli.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:            aws.String(s.bucket),
		Key:               aws.String(key),
		Body:              counter,
		ChecksumAlgorithm: s3types.ChecksumAlgorithmSha256,
	})
	if err != nil {
		log.Warnf("uploading file %v: %v", key, err)

		s.lock.Lock()
		defer s.lock.Unlock()

		delete(s.workingTempFiles, fileName)
		return types.FileInfo{}, err
	}

	return s.onCreateFinished(key, counter.Count(), cdssdk.NewFullHash(hashStr.Sum()))
}

func (s *ShardStore) createTempFile() (string, string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	tmpDir := JoinKey(s.cfg.Root, TempDir)
	tmpName := os2.GenerateRandomFileName(20)

	s.workingTempFiles[tmpName] = true
	return JoinKey(tmpDir, tmpName), tmpName
}

func (s *ShardStore) onCreateFinished(tempFilePath string, size int64, hash cdssdk.FileHash) (types.FileInfo, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	defer delete(s.workingTempFiles, filepath.Base(tempFilePath))
	defer func() {
		// 不管是否成功。即使失败了也有定时清理机制去兜底
		s.cli.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(tempFilePath),
		})
	}()

	log := s.getLogger()

	log.Debugf("write file %v finished, size: %v, hash: %v", tempFilePath, size, hash)

	blockDir := s.getFileDirFromHash(hash)
	newPath := JoinKey(blockDir, string(hash))

	_, err := s.cli.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket:     aws.String(s.bucket),
		CopySource: aws.String(tempFilePath),
		Key:        aws.String(newPath),
	})
	if err != nil {
		log.Warnf("copy file %v to %v: %v", tempFilePath, newPath, err)
		return types.FileInfo{}, err
	}

	return types.FileInfo{
		Hash:        hash,
		Size:        size,
		Description: newPath,
	}, nil
}

func (s *ShardStore) onCreateFailed(key string, fileName string) {
	// 不管是否成功。即使失败了也有定时清理机制去兜底
	s.cli.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})

	s.lock.Lock()
	defer s.lock.Unlock()

	delete(s.workingTempFiles, fileName)
}

// 使用NewOpen函数创建Option对象
func (s *ShardStore) Open(opt types.OpenOption) (io.ReadCloser, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	filePath := s.getFilePathFromHash(opt.FileHash)

	rngStr := fmt.Sprintf("bytes=%d-", opt.Offset)
	if opt.Length >= 0 {
		rngStr += fmt.Sprintf("%d", opt.Offset+opt.Length-1)
	}

	resp, err := s.cli.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(filePath),
		Range:  aws.String(rngStr),
	})
	if err != nil {
		s.getLogger().Warnf("get file %v: %v", filePath, err)
		return nil, err
	}

	return resp.Body, nil
}

func (s *ShardStore) Info(hash cdssdk.FileHash) (types.FileInfo, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	filePath := s.getFilePathFromHash(hash)
	info, err := s.cli.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(filePath),
	})
	if err != nil {
		s.getLogger().Warnf("get file %v: %v", filePath, err)
		return types.FileInfo{}, err
	}

	return types.FileInfo{
		Hash:        hash,
		Size:        *info.ContentLength,
		Description: filePath,
	}, nil
}

func (s *ShardStore) ListAll() ([]types.FileInfo, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	var infos []types.FileInfo

	blockDir := JoinKey(s.cfg.Root, BlocksDir)

	var marker *string
	for {
		resp, err := s.cli.ListObjects(context.Background(), &s3.ListObjectsInput{
			Bucket: aws.String(s.bucket),
			Prefix: aws.String(blockDir),
			Marker: marker,
		})

		if err != nil {
			s.getLogger().Warnf("list objects: %v", err)
			return nil, err
		}

		for _, obj := range resp.Contents {
			key := BaseKey(*obj.Key)

			fileHash, err := cdssdk.ParseHash(key)
			if err != nil {
				continue
			}

			infos = append(infos, types.FileInfo{
				Hash:        fileHash,
				Size:        *obj.Size,
				Description: *obj.Key,
			})
		}

		if !*resp.IsTruncated {
			break
		}

		marker = resp.NextMarker
	}

	return infos, nil
}

func (s *ShardStore) GC(avaiables []cdssdk.FileHash) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	avais := make(map[cdssdk.FileHash]bool)
	for _, hash := range avaiables {
		avais[hash] = true
	}

	blockDir := JoinKey(s.cfg.Root, BlocksDir)

	var deletes []s3types.ObjectIdentifier
	var marker *string
	for {
		resp, err := s.cli.ListObjects(context.Background(), &s3.ListObjectsInput{
			Bucket: aws.String(s.bucket),
			Prefix: aws.String(blockDir),
			Marker: marker,
		})

		if err != nil {
			s.getLogger().Warnf("list objects: %v", err)
			return err
		}

		for _, obj := range resp.Contents {
			key := BaseKey(*obj.Key)
			fileHash, err := cdssdk.ParseHash(key)
			if err != nil {
				continue
			}

			if !avais[fileHash] {
				deletes = append(deletes, s3types.ObjectIdentifier{
					Key: obj.Key,
				})
			}
		}

		if !*resp.IsTruncated {
			break
		}

		marker = resp.NextMarker
	}

	cnt := 0
	if len(deletes) > 0 {
		resp, err := s.cli.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
			Bucket: aws.String(s.bucket),
			Delete: &s3types.Delete{
				Objects: deletes,
			},
		})
		if err != nil {
			s.getLogger().Warnf("delete objects: %v", err)
			return err
		}

		cnt = len(resp.Deleted)
	}

	s.getLogger().Infof("purge %d files", cnt)
	// TODO 无法保证原子性，所以删除失败只打日志
	return nil
}

func (s *ShardStore) Stats() types.Stats {
	// TODO 统计本地存储的相关信息
	return types.Stats{
		Status: types.StatusOK,
	}
}

func (s *ShardStore) BypassUploaded(info types.BypassFileInfo) error {
	if info.FileHash == "" {
		return fmt.Errorf("empty file hash is not allowed by this shard store")
	}

	s.lock.Lock()
	defer s.lock.Unlock()
	defer func() {
		// 不管是否成功。即使失败了也有定时清理机制去兜底
		s.cli.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(info.TempFilePath),
		})
	}()

	log := s.getLogger()

	log.Debugf("%v bypass uploaded, size: %v, hash: %v", info.TempFilePath, info.Size, info.FileHash)

	blockDir := s.getFileDirFromHash(info.FileHash)
	newPath := JoinKey(blockDir, string(info.FileHash))

	_, err := s.cli.CopyObject(context.Background(), &s3.CopyObjectInput{
		CopySource: aws.String(JoinKey(s.bucket, info.TempFilePath)),
		Bucket:     aws.String(s.bucket),
		Key:        aws.String(newPath),
	})
	if err != nil {
		log.Warnf("copy file %v to %v: %v", info.TempFilePath, newPath, err)
		return fmt.Errorf("copy file: %w", err)
	}

	return nil
}

func (s *ShardStore) getLogger() logger.Logger {
	return logger.WithField("ShardStore", "S3").WithField("Storage", s.svc.Detail.Storage.String())
}

func (s *ShardStore) getFileDirFromHash(hash cdssdk.FileHash) string {
	return JoinKey(s.cfg.Root, BlocksDir, hash.GetHashPrefix(2))
}

func (s *ShardStore) getFilePathFromHash(hash cdssdk.FileHash) string {
	return JoinKey(s.cfg.Root, BlocksDir, hash.GetHashPrefix(2), string(hash))
}
