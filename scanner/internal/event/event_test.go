package event

/*
import (
	"testing"

	"github.com/samber/lo"
	. "github.com/smartystreets/goconvey/convey"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

func Test_chooseSoManyNodes(t *testing.T) {
	testcases := []struct {
		title           string
		allNodes        []*StorageLoadInfo
		count           int
		expectedHubIDs []cdssdk.HubID
	}{
		{
			title: "节点数量充足",
			allNodes: []*StorageLoadInfo{
				{Storage: cdssdk.Node{HubID: cdssdk.HubID(1)}},
				{Storage: cdssdk.Node{HubID: cdssdk.HubID(2)}},
			},
			count:           2,
			expectedHubIDs: []cdssdk.HubID{1, 2},
		},
		{
			title: "节点数量超过",
			allNodes: []*StorageLoadInfo{
				{Storage: cdssdk.Node{HubID: cdssdk.HubID(1)}},
				{Storage: cdssdk.Node{HubID: cdssdk.HubID(2)}},
				{Storage: cdssdk.Node{HubID: cdssdk.HubID(3)}},
			},
			count:           2,
			expectedHubIDs: []cdssdk.HubID{1, 2},
		},
		{
			title: "只有一个节点，节点数量不够",
			allNodes: []*StorageLoadInfo{
				{Storage: cdssdk.Node{HubID: cdssdk.HubID(1)}},
			},
			count:           3,
			expectedHubIDs: []cdssdk.HubID{1, 1, 1},
		},
		{
			title: "多个同地区节点，节点数量不够",
			allNodes: []*StorageLoadInfo{
				{Storage: cdssdk.Node{HubID: cdssdk.HubID(1)}},
				{Storage: cdssdk.Node{HubID: cdssdk.HubID(2)}},
			},
			count:           5,
			expectedHubIDs: []cdssdk.HubID{1, 1, 1, 2, 2},
		},
		{
			title: "节点数量不够，且在不同地区",
			allNodes: []*StorageLoadInfo{
				{Storage: cdssdk.Node{HubID: cdssdk.HubID(1), LocationID: cdssdk.LocationID(1)}},
				{Storage: cdssdk.Node{HubID: cdssdk.HubID(2), LocationID: cdssdk.LocationID(2)}},
			},
			count:           5,
			expectedHubIDs: []cdssdk.HubID{1, 2, 1, 2, 1},
		},
	}

	for _, test := range testcases {
		Convey(test.title, t, func() {
			var t CheckPackageRedundancy
			chosenNodes := t.chooseSoManyNodes(test.count, test.allNodes)

			chosenHubIDs := lo.Map(chosenNodes, func(item *StorageLoadInfo, idx int) cdssdk.HubID { return item.Storage.HubID })

			So(chosenHubIDs, ShouldResemble, test.expectedHubIDs)
		})
	}
}
*/
