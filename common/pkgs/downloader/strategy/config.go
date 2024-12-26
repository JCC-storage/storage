package strategy

type Config struct {
	// 当到下载节点的延迟高于这个值时，该节点在评估时会有更高的分数惩罚，单位：ms
	HighLatencyHubMs float64 `json:"highLatencyHubMs"`
}
