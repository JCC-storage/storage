package lockprovider

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
)

func Test_ShardStoreLock(t *testing.T) {
	cases := []struct {
		title     string
		initLocks []distlock.Lock
		doLock    distlock.Lock
		wantOK    bool
	}{
		{
			title: "同节点，同一个Buzy锁",
			initLocks: []distlock.Lock{
				{
					Path: []string{ShardStoreLockPathPrefix, "hub1"},
					Name: ShardStoreBuzyLock,
				},
			},
			doLock: distlock.Lock{
				Path: []string{ShardStoreLockPathPrefix, "hub1"},
				Name: ShardStoreBuzyLock,
			},
			wantOK: true,
		},
		{
			title: "同节点，同一个GC锁",
			initLocks: []distlock.Lock{
				{
					Path: []string{ShardStoreLockPathPrefix, "hub1"},
					Name: ShardStoreGCLock,
				},
			},
			doLock: distlock.Lock{
				Path: []string{ShardStoreLockPathPrefix, "hub1"},
				Name: ShardStoreGCLock,
			},
			wantOK: true,
		},
		{
			title: "同时设置Buzy和GC",
			initLocks: []distlock.Lock{
				{
					Path:   []string{ShardStoreLockPathPrefix, "hub1"},
					Name:   ShardStoreBuzyLock,
					Target: *NewStringLockTarget(),
				},
			},
			doLock: distlock.Lock{
				Path:   []string{ShardStoreLockPathPrefix, "hub1"},
				Name:   ShardStoreGCLock,
				Target: *NewStringLockTarget(),
			},
			wantOK: false,
		},
	}

	for _, ca := range cases {
		Convey(ca.title, t, func() {
			ipfsLock := NewShardStoreLock()

			for _, l := range ca.initLocks {
				ipfsLock.Lock("req1", l)
			}

			err := ipfsLock.CanLock(ca.doLock)
			if ca.wantOK {
				So(err, ShouldBeNil)
			} else {
				So(err, ShouldNotBeNil)
			}
		})
	}

	Convey("解锁", t, func() {
		ipfsLock := NewShardStoreLock()

		lock := distlock.Lock{
			Path: []string{ShardStoreLockPathPrefix, "hub1"},
			Name: ShardStoreBuzyLock,
		}

		ipfsLock.Lock("req1", lock)

		err := ipfsLock.CanLock(lock)
		So(err, ShouldBeNil)

		ipfsLock.Unlock("req1", lock)

		lock = distlock.Lock{
			Path: []string{ShardStoreLockPathPrefix, "hub1"},
			Name: ShardStoreGCLock,
		}
		err = ipfsLock.CanLock(lock)
		So(err, ShouldBeNil)
	})

}
