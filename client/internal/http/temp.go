package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type TempService struct {
	*Server
}

func (s *Server) Temp() *TempService {
	return &TempService{
		Server: s,
	}
}

type TempListDetailsResp struct {
	Buckets []BucketDetail `json:"buckets"`
}
type BucketDetail struct {
	BucketID    cdssdk.BucketID `json:"bucketID"`
	Name        string          `json:"name"`
	ObjectCount int             `json:"objectCount"`
}

func (s *TempService) ListDetails(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Bucket.ListBucketsDetails")

	bkts, err := s.svc.BucketSvc().GetUserBuckets(1)
	if err != nil {
		log.Warnf("getting user buckets: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get user buckets failed"))
		return
	}

	details := make([]BucketDetail, len(bkts))
	for i := range bkts {
		details[i].BucketID = bkts[i].BucketID
		details[i].Name = bkts[i].Name
		objs, err := s.getBucketObjects(bkts[i].BucketID)
		if err != nil {
			log.Warnf("getting bucket objects: %s", err.Error())
			ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get bucket objects failed"))
			return
		}
		details[i].ObjectCount = len(objs)
	}

	ctx.JSON(http.StatusOK, OK(TempListDetailsResp{
		Buckets: details,
	}))
}

type TempGetObjects struct {
	BucketID cdssdk.BucketID `form:"bucketID"`
}
type BucketGetObjectsResp struct {
	Objects []cdssdk.Object `json:"objects"`
}

func (s *TempService) GetObjects(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Bucket.ListBucketsDetails")

	var req TempGetObjects
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding query: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	objs, err := s.getBucketObjects(req.BucketID)
	if err != nil {
		log.Warnf("getting bucket objects: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get bucket objects failed"))
		return
	}

	ctx.JSON(http.StatusOK, OK(BucketGetObjectsResp{
		Objects: objs,
	}))
}

type TempGetObjectDetail struct {
	ObjectID cdssdk.ObjectID `form:"objectID"`
}
type TempGetObjectDetailResp struct {
	Blocks []ObjectBlockDetail `json:"blocks"`
}
type ObjectBlockDetail struct {
	ObjectID     cdssdk.ObjectID `json:"objectID"`
	Type         string          `json:"type"`
	FileHash     cdssdk.FileHash `json:"fileHash"`
	LocationType string          `json:"locationType"`
	LocationName string          `json:"locationName"`
}

func (s *TempService) GetObjectDetail(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Object.GetObjectDetail")

	var req TempGetObjectDetail
	if err := ctx.ShouldBindQuery(&req); err != nil {
		log.Warnf("binding query: %s", err.Error())
		ctx.JSON(http.StatusBadRequest, Failed(errorcode.BadArgument, "missing argument or invalid argument"))
		return
	}

	details, err := s.svc.ObjectSvc().GetObjectDetail(req.ObjectID)
	if err != nil {
		log.Warnf("getting object detail: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get object detail failed"))
		return
	}
	if details == nil {
		log.Warnf("object not found")
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "object not found"))
		return
	}

	var allStgIDs []cdssdk.StorageID
	allStgIDs = append(allStgIDs, details.PinnedAt...)
	for _, b := range details.Blocks {
		allStgIDs = append(allStgIDs, b.StorageID)
	}

	allStgIDs = lo.Uniq(allStgIDs)

	getStgs, err := s.svc.StorageSvc().GetDetails(allStgIDs)
	if err != nil {
		log.Warnf("getting nodes: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get nodes failed"))
		return
	}

	allStgs := make(map[cdssdk.StorageID]cdssdk.Storage)
	for _, n := range getStgs {
		if n != nil {
			allStgs[n.Storage.StorageID] = n.Storage
		}
	}

	var blocks []ObjectBlockDetail

	for _, stgID := range details.PinnedAt {
		blocks = append(blocks, ObjectBlockDetail{
			Type:         "Rep",
			FileHash:     details.Object.FileHash,
			LocationType: "Agent",
			LocationName: allStgs[stgID].Name,
		})
	}

	switch details.Object.Redundancy.(type) {
	case *cdssdk.NoneRedundancy:
		for _, blk := range details.Blocks {
			if !lo.Contains(details.PinnedAt, blk.StorageID) {
				blocks = append(blocks, ObjectBlockDetail{
					Type:         "Rep",
					FileHash:     blk.FileHash,
					LocationType: "Agent",
					LocationName: allStgs[blk.StorageID].Name,
				})
			}
		}
	case *cdssdk.RepRedundancy:
		for _, blk := range details.Blocks {
			if !lo.Contains(details.PinnedAt, blk.StorageID) {
				blocks = append(blocks, ObjectBlockDetail{
					Type:         "Rep",
					FileHash:     blk.FileHash,
					LocationType: "Agent",
					LocationName: allStgs[blk.StorageID].Name,
				})
			}
		}

	case *cdssdk.ECRedundancy:
		for _, blk := range details.Blocks {
			blocks = append(blocks, ObjectBlockDetail{
				Type:         "Block",
				FileHash:     blk.FileHash,
				LocationType: "Agent",
				LocationName: allStgs[blk.StorageID].Name,
			})
		}
	}

	ctx.JSON(http.StatusOK, OK(TempGetObjectDetailResp{
		Blocks: blocks,
	}))
}

func (s *TempService) getBucketObjects(bktID cdssdk.BucketID) ([]cdssdk.Object, error) {
	pkgs, err := s.svc.PackageSvc().GetBucketPackages(1, bktID)
	if err != nil {
		return nil, err
	}

	var allObjs []cdssdk.Object
	for _, pkg := range pkgs {
		objs, err := s.svc.ObjectSvc().GetPackageObjects(1, pkg.PackageID)
		if err != nil {
			return nil, err
		}
		allObjs = append(allObjs, objs...)
	}

	return allObjs, nil
}

type TempGetDatabaseAll struct {
}
type TempGetDatabaseAllResp struct {
	Buckets []BucketDetail      `json:"buckets"`
	Objects []BucketObject      `json:"objects"`
	Blocks  []ObjectBlockDetail `json:"blocks"`
}
type BucketObject struct {
	cdssdk.Object
	BucketID cdssdk.BucketID `json:"bucketID"`
}

func (s *TempService) GetDatabaseAll(ctx *gin.Context) {
	log := logger.WithField("HTTP", "Temp.GetDatabaseAll")

	db, err := s.svc.ObjectSvc().GetDatabaseAll()
	if err != nil {
		log.Warnf("getting database all: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get database all failed"))
		return
	}

	var allStgIDs []cdssdk.StorageID
	for _, obj := range db.Objects {
		allStgIDs = append(allStgIDs, obj.PinnedAt...)
		for _, blk := range obj.Blocks {
			allStgIDs = append(allStgIDs, blk.StorageID)
		}
	}

	getStgs, err := s.svc.StorageSvc().GetDetails(allStgIDs)
	if err != nil {
		log.Warnf("getting nodes: %s", err.Error())
		ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get nodes failed"))
		return
	}
	allStgs := make(map[cdssdk.StorageID]cdssdk.Storage)
	for _, n := range getStgs {
		if n != nil {
			allStgs[n.Storage.StorageID] = n.Storage
		}
	}

	bkts := make(map[cdssdk.BucketID]*BucketDetail)
	for _, bkt := range db.Buckets {
		bkts[bkt.BucketID] = &BucketDetail{
			BucketID:    bkt.BucketID,
			Name:        bkt.Name,
			ObjectCount: 0,
		}
	}

	type PackageDetail struct {
		Package cdssdk.Package
		// Loaded  []cdssdk.Node
	}
	pkgs := make(map[cdssdk.PackageID]*PackageDetail)
	for _, pkg := range db.Packages {
		p := PackageDetail{
			Package: pkg,
			// Loaded:  make([]cdssdk.Node, 0),
		}

		// loaded, err := s.svc.PackageSvc().GetLoadedNodes(1, pkg.PackageID)
		// if err != nil {
		// 	log.Warnf("getting loaded nodes: %s", err.Error())
		// 	ctx.JSON(http.StatusOK, Failed(errorcode.OperationFailed, "get loaded nodes failed"))
		// 	return
		// }

		// for _, hubID := range loaded {
		// 	p.Loaded = append(p.Loaded, allNodes[hubID])
		// }

		pkgs[pkg.PackageID] = &p
	}

	var objs []BucketObject
	for _, obj := range db.Objects {
		o := BucketObject{
			Object:   obj.Object,
			BucketID: pkgs[obj.Object.PackageID].Package.BucketID,
		}
		objs = append(objs, o)
	}

	var blocks []ObjectBlockDetail
	for _, obj := range db.Objects {
		bkts[pkgs[obj.Object.PackageID].Package.BucketID].ObjectCount++

		for _, hubID := range obj.PinnedAt {
			blocks = append(blocks, ObjectBlockDetail{
				ObjectID:     obj.Object.ObjectID,
				Type:         "Rep",
				FileHash:     obj.Object.FileHash,
				LocationType: "Agent",
				LocationName: allStgs[hubID].Name,
			})
		}

		switch obj.Object.Redundancy.(type) {
		case *cdssdk.NoneRedundancy:
			for _, blk := range obj.Blocks {
				if !lo.Contains(obj.PinnedAt, blk.StorageID) {
					blocks = append(blocks, ObjectBlockDetail{
						ObjectID:     obj.Object.ObjectID,
						Type:         "Rep",
						FileHash:     blk.FileHash,
						LocationType: "Agent",
						LocationName: allStgs[blk.StorageID].Name,
					})
				}
			}
		case *cdssdk.RepRedundancy:
			for _, blk := range obj.Blocks {
				if !lo.Contains(obj.PinnedAt, blk.StorageID) {
					blocks = append(blocks, ObjectBlockDetail{
						ObjectID:     obj.Object.ObjectID,
						Type:         "Rep",
						FileHash:     blk.FileHash,
						LocationType: "Agent",
						LocationName: allStgs[blk.StorageID].Name,
					})
				}
			}

		case *cdssdk.ECRedundancy:
			for _, blk := range obj.Blocks {
				blocks = append(blocks, ObjectBlockDetail{
					ObjectID:     obj.Object.ObjectID,
					Type:         "Block",
					FileHash:     blk.FileHash,
					LocationType: "Agent",
					LocationName: allStgs[blk.StorageID].Name,
				})
			}
		}

		// for _, node := range pkgs[obj.Object.PackageID].Loaded {
		// 	blocks = append(blocks, ObjectBlockDetail{
		// 		ObjectID:     obj.Object.ObjectID,
		// 		Type:         "Rep",
		// 		FileHash:     obj.Object.FileHash,
		// 		LocationType: "Storage",
		// 		LocationName: allNodes[node.HubID].Name,
		// 	})
		// }

	}

	ctx.JSON(http.StatusOK, OK(TempGetDatabaseAllResp{
		Buckets: lo.Map(lo.Values(bkts), func(b *BucketDetail, _ int) BucketDetail { return *b }),
		Objects: objs,
		Blocks:  blocks,
	}))
}

func initTemp(rt gin.IRoutes, s *Server) {
	rt.GET("/bucket/listDetails", s.Temp().ListDetails)
	rt.GET("/bucket/getObjects", s.Temp().GetObjects)
	rt.GET("/object/getDetail", s.Temp().GetObjectDetail)
	rt.GET("/temp/getDatabaseAll", s.Temp().GetDatabaseAll)
}

func auth(ctx *gin.Context) {
	token := ctx.Request.Header.Get("X-CDS-Auth")
	if token != "cloudream@123" {
		ctx.AbortWithStatus(http.StatusUnauthorized)
	}
}
