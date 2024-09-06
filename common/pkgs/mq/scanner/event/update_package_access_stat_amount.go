package event

import cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

type UpdatePackageAccessStatAmount struct {
	EventBase
	PackageIDs []cdssdk.PackageID `json:"packageIDs"`
}

func NewUpdatePackageAccessStatAmount(packageIDs []cdssdk.PackageID) *UpdatePackageAccessStatAmount {
	return &UpdatePackageAccessStatAmount{
		PackageIDs: packageIDs,
	}
}

func init() {
	Register[*UpdatePackageAccessStatAmount]()
}
