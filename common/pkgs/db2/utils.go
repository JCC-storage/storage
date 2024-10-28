package db2

import (
	"gorm.io/gorm"
)

const (
	maxPlaceholderCount = 65535
)

func BatchNamedExec[T any](ctx SQLContext, sql string, argCnt int, arr []T, callback func(result *gorm.DB) bool) error {
	if argCnt == 0 {
		result := ctx.Exec(sql, toInterfaceSlice(arr)...)
		if result.Error != nil {
			return result.Error
		}

		if callback != nil {
			callback(result)
		}

		return nil
	}

	batchSize := maxPlaceholderCount / argCnt
	for len(arr) > 0 {
		curBatchSize := min(batchSize, len(arr))

		result := ctx.Exec(sql, toInterfaceSlice(arr[:curBatchSize])...)
		if result.Error != nil {
			return result.Error
		}
		if callback != nil && !callback(result) {
			return nil
		}

		arr = arr[curBatchSize:]
	}

	return nil
}

// 将 []T 转换为 []interface{}
func toInterfaceSlice[T any](arr []T) []interface{} {
	interfaceSlice := make([]interface{}, len(arr))
	for i, v := range arr {
		interfaceSlice[i] = v
	}
	return interfaceSlice
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
