package s3

type SharedStoreDesc struct {
}

func (d *SharedStoreDesc) Enabled() bool {
	return false
}

func (d *SharedStoreDesc) HasBypassWrite() bool {
	return false
}
