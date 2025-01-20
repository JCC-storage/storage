package types

type ECMultiplier interface {
	// 进行EC运算，coef * inputs。coef为编码矩阵，inputs为待编码数据，chunkSize为分块大小。
	// 输出为每一个块文件的路径，数组长度 = len(coef)
	Multiply(coef [][]byte, inputs []HTTPReqeust, chunkSize int64) ([]string, error)
	// 完成计算
	Complete()
	// 取消计算。如果已经调用了Complete，则应该无任何影响
	Abort()
}
