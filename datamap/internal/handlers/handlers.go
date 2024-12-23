package handlers

import (
	"github.com/gin-gonic/gin"
	"gitlink.org.cn/cloudream/storage/datamap/internal/db"
	"gitlink.org.cn/cloudream/storage/datamap/internal/models"
	"net/http"
)

// GetStorageData 获取 Storage 表中的全部数据
func GetStorageData(c *gin.Context) {
	repo := models.NewStorageRepository(db.DB)
	storages, err := repo.GetAllStorages()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch storage data"})
		return
	}
	c.JSON(http.StatusOK, storages)
}

// GetBlockDistributionData 获取 BlockDistribution 表中的全部数据
func GetBlockDistributionData(c *gin.Context) {
	repo := models.NewBlockDistributionRepository(db.DB)
	blocks, err := repo.GetAllBlocks()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch block distribution data"})
		return
	}
	c.JSON(http.StatusOK, blocks)
}
