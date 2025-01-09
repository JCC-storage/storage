package s3

type SharedStoreDesc struct {
}

func (d *SharedStoreDesc) Enabled() bool {
	return false
}

func (d *SharedStoreDesc) HasBypassNotifier() bool {
	return false
}
