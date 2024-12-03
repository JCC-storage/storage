package types

type TempStore interface {
	// 生成并注册一个临时文件路径，在Commited或Drop之前，此文件不会被清理。
	CreateTemp() string
	// 指示一个临时文件已经被移动作它用，不需要再关注它了（也不需要删除这个文件）。
	Commited(filePath string)
	// 临时文件被放弃，可以删除这个文件了。如果提前调用了Commited，则此函数应该什么也不做。
	Drop(filePath string)
}
