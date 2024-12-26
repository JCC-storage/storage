package cmdline

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"
	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/parser"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

func init() {
	rootCmd.AddCommand(&cobra.Command{
		Use:   "test",
		Short: "test",
		Run: func(cmd *cobra.Command, args []string) {
			coorCli, err := stgglb.CoordinatorMQPool.Acquire()
			if err != nil {
				panic(err)
			}
			defer stgglb.CoordinatorMQPool.Release(coorCli)

			stgs, err := coorCli.GetStorageDetails(coormq.ReqGetStorageDetails([]cdssdk.StorageID{1, 2, 3}))
			if err != nil {
				panic(err)
			}

			ft := ioswitch2.NewFromTo()
			ft.SegmentParam = cdssdk.NewSegmentRedundancy(1024*100*3, 3)
			ft.AddFrom(ioswitch2.NewFromShardstore("FullE58B075E9F7C5744CB1C2CBBECC30F163DE699DCDA94641DDA34A0C2EB01E240", *stgs.Storages[1].MasterHub, stgs.Storages[1].Storage, ioswitch2.SegmentStream(0)))
			ft.AddFrom(ioswitch2.NewFromShardstore("FullEA14D17544786427C3A766F0C5E6DEB221D00D3DE1875BBE3BD0AD5C8118C1A0", *stgs.Storages[1].MasterHub, stgs.Storages[1].Storage, ioswitch2.SegmentStream(1)))
			ft.AddFrom(ioswitch2.NewFromShardstore("Full4D142C458F2399175232D5636235B09A84664D60869E925EB20FFBE931045BDD", *stgs.Storages[1].MasterHub, stgs.Storages[1].Storage, ioswitch2.SegmentStream(2)))
			ft.AddTo(ioswitch2.NewToShardStore(*stgs.Storages[2].MasterHub, *stgs.Storages[2], ioswitch2.RawStream(), "0"))
			// ft.AddFrom(ioswitch2.NewFromShardstore("CA56E5934859E0220D1F3B848F41619D937D7B874D4EBF63A6CC98D2D8E3280F", *stgs.Storages[0].MasterHub, stgs.Storages[0].Storage, ioswitch2.RawStream()))
			// ft.AddTo(ioswitch2.NewToShardStore(*stgs.Storages[1].MasterHub, stgs.Storages[1].Storage, ioswitch2.SegmentStream(0), "0"))
			// ft.AddTo(ioswitch2.NewToShardStoreWithRange(*stgs.Storages[1].MasterHub, *stgs.Storages[1], ioswitch2.SegmentStream(1), "1", exec.Range{Offset: 1}))
			// ft.AddTo(ioswitch2.NewToShardStore(*stgs.Storages[1].MasterHub, *stgs.Storages[1], ioswitch2.SegmentStream(0), "0"))
			// ft.AddTo(ioswitch2.NewToShardStore(*stgs.Storages[1].MasterHub, *stgs.Storages[1], ioswitch2.SegmentStream(1), "1"))
			// ft.AddTo(ioswitch2.NewToShardStore(*stgs.Storages[1].MasterHub, *stgs.Storages[1], ioswitch2.SegmentStream(2), "2"))

			plans := exec.NewPlanBuilder()
			err = parser.Parse(ft, plans)
			if err != nil {
				panic(err)
			}

			fmt.Printf("plans: %v\n", plans)

			exec := plans.Execute(exec.NewExecContext())

			fut := future.NewSetVoid()
			go func() {
				mp, err := exec.Wait(context.Background())
				if err != nil {
					panic(err)
				}

				fmt.Printf("0: %v, 1: %v, 2: %v\n", mp["0"], mp["1"], mp["2"])
				fut.SetVoid()
			}()

			fut.Wait(context.TODO())
		},
	})

	rootCmd.AddCommand(&cobra.Command{
		Use:   "test32",
		Short: "test32",
		Run: func(cmd *cobra.Command, args []string) {
			coorCli, err := stgglb.CoordinatorMQPool.Acquire()
			if err != nil {
				panic(err)
			}
			defer stgglb.CoordinatorMQPool.Release(coorCli)

			stgs, err := coorCli.GetStorageDetails(coormq.ReqGetStorageDetails([]cdssdk.StorageID{1, 2}))
			if err != nil {
				panic(err)
			}

			ft := ioswitch2.NewFromTo()
			ft.SegmentParam = cdssdk.NewSegmentRedundancy(1293, 3)
			ft.AddFrom(ioswitch2.NewFromShardstore("4E69A8B8CD9F42EDE371DA94458BADFB2308AFCA736AA393784A3D81F4746377", *stgs.Storages[0].MasterHub, stgs.Storages[0].Storage, ioswitch2.RawStream()))
			// ft.AddTo(ioswitch2.NewToShardStore(*stgs.Storages[1].MasterHub, stgs.Storages[1].Storage, ioswitch2.SegmentStream(0), "0"))
			ft.AddTo(ioswitch2.NewToShardStoreWithRange(*stgs.Storages[1].MasterHub, *stgs.Storages[1], ioswitch2.SegmentStream(1), "1", exec.Range{Offset: 1}))
			ft.AddTo(ioswitch2.NewToShardStore(*stgs.Storages[1].MasterHub, *stgs.Storages[1], ioswitch2.SegmentStream(2), "2"))

			plans := exec.NewPlanBuilder()
			err = parser.Parse(ft, plans)
			if err != nil {
				panic(err)
			}

			fmt.Printf("plans: %v\n", plans)

			exec := plans.Execute(exec.NewExecContext())

			fut := future.NewSetVoid()
			go func() {
				mp, err := exec.Wait(context.Background())
				if err != nil {
					panic(err)
				}

				fmt.Printf("0: %v, 1: %v, 2: %v\n", mp["0"], mp["1"], mp["2"])
				fut.SetVoid()
			}()

			fut.Wait(context.TODO())
		},
	})
	rootCmd.AddCommand(&cobra.Command{
		Use:   "test1",
		Short: "test1",
		Run: func(cmd *cobra.Command, args []string) {
			coorCli, err := stgglb.CoordinatorMQPool.Acquire()
			if err != nil {
				panic(err)
			}
			defer stgglb.CoordinatorMQPool.Release(coorCli)

			stgs, err := coorCli.GetStorageDetails(coormq.ReqGetStorageDetails([]cdssdk.StorageID{1, 2}))
			if err != nil {
				panic(err)
			}

			ft := ioswitch2.NewFromTo()
			ft.SegmentParam = cdssdk.NewSegmentRedundancy(1293, 3)
			ft.ECParam = &cdssdk.DefaultECRedundancy
			ft.AddFrom(ioswitch2.NewFromShardstore("22CC59CE3297F78F2D20DC1E33181B77F21E6782097C94E1664F99F129834069", *stgs.Storages[1].MasterHub, stgs.Storages[1].Storage, ioswitch2.SegmentStream(0)))
			ft.AddFrom(ioswitch2.NewFromShardstore("5EAC20EB3EBC7B5FA176C5BD1C01041FB2A6D14C35D6A232CA83D7F1E4B01ADE", *stgs.Storages[1].MasterHub, stgs.Storages[1].Storage, ioswitch2.SegmentStream(1)))
			ft.AddFrom(ioswitch2.NewFromShardstore("A9BC1802F37100C80C72A1D6E8F53C0E0B73F85F99153D8C78FB01CEC9D8D903", *stgs.Storages[1].MasterHub, stgs.Storages[1].Storage, ioswitch2.SegmentStream(2)))

			toDrv, drvStr := ioswitch2.NewToDriverWithRange(ioswitch2.RawStream(), exec.NewRange(0, 1293))
			ft.AddTo(toDrv)
			ft.AddTo(ioswitch2.NewToShardStore(*stgs.Storages[0].MasterHub, *stgs.Storages[0], ioswitch2.ECStream(0), "EC0"))
			ft.AddTo(ioswitch2.NewToShardStore(*stgs.Storages[0].MasterHub, *stgs.Storages[0], ioswitch2.ECStream(1), "EC1"))
			ft.AddTo(ioswitch2.NewToShardStore(*stgs.Storages[0].MasterHub, *stgs.Storages[0], ioswitch2.ECStream(2), "EC2"))

			plans := exec.NewPlanBuilder()
			err = parser.Parse(ft, plans)
			if err != nil {
				panic(err)
			}

			fmt.Printf("plans: %v\n", plans)

			exec := plans.Execute(exec.NewExecContext())

			fut := future.NewSetVoid()
			go func() {
				mp, err := exec.Wait(context.Background())
				if err != nil {
					panic(err)
				}

				for k, v := range mp {
					fmt.Printf("%s: %v\n", k, v)
				}

				fut.SetVoid()
			}()

			go func() {
				str, err := exec.BeginRead(drvStr)
				if err != nil {
					panic(err)
				}

				data, err := io.ReadAll(str)
				if err != nil {
					panic(err)
				}

				fmt.Printf("read(%v): %s\n", len(data), string(data))
			}()

			fut.Wait(context.TODO())
		},
	})

	rootCmd.AddCommand(&cobra.Command{
		Use:   "test4",
		Short: "test4",
		Run: func(cmd *cobra.Command, args []string) {
			coorCli, err := stgglb.CoordinatorMQPool.Acquire()
			if err != nil {
				panic(err)
			}
			defer stgglb.CoordinatorMQPool.Release(coorCli)

			stgs, err := coorCli.GetStorageDetails(coormq.ReqGetStorageDetails([]cdssdk.StorageID{1, 2}))
			if err != nil {
				panic(err)
			}

			ft := ioswitch2.NewFromTo()
			ft.ECParam = &cdssdk.DefaultECRedundancy
			ft.AddFrom(ioswitch2.NewFromShardstore("4E69A8B8CD9F42EDE371DA94458BADFB2308AFCA736AA393784A3D81F4746377", *stgs.Storages[1].MasterHub, stgs.Storages[1].Storage, ioswitch2.RawStream()))
			ft.AddTo(ioswitch2.NewToShardStore(*stgs.Storages[0].MasterHub, *stgs.Storages[0], ioswitch2.ECStream(0), "EC0"))
			ft.AddTo(ioswitch2.NewToShardStore(*stgs.Storages[0].MasterHub, *stgs.Storages[0], ioswitch2.ECStream(1), "EC1"))
			ft.AddTo(ioswitch2.NewToShardStore(*stgs.Storages[0].MasterHub, *stgs.Storages[0], ioswitch2.ECStream(2), "EC2"))

			plans := exec.NewPlanBuilder()
			err = parser.Parse(ft, plans)
			if err != nil {
				panic(err)
			}

			fmt.Printf("plans: %v\n", plans)

			exec := plans.Execute(exec.NewExecContext())

			fut := future.NewSetVoid()
			go func() {
				mp, err := exec.Wait(context.Background())
				if err != nil {
					panic(err)
				}

				for k, v := range mp {
					fmt.Printf("%s: %v\n", k, v)
				}

				fut.SetVoid()
			}()

			fut.Wait(context.TODO())
		},
	})

	rootCmd.AddCommand(&cobra.Command{
		Use:   "test11",
		Short: "test11",
		Run: func(cmd *cobra.Command, args []string) {
			coorCli, err := stgglb.CoordinatorMQPool.Acquire()
			if err != nil {
				panic(err)
			}
			defer stgglb.CoordinatorMQPool.Release(coorCli)

			stgs, err := coorCli.GetStorageDetails(coormq.ReqGetStorageDetails([]cdssdk.StorageID{1, 2}))
			if err != nil {
				panic(err)
			}

			ft := ioswitch2.NewFromTo()
			ft.SegmentParam = cdssdk.NewSegmentRedundancy(1293, 3)
			ft.ECParam = &cdssdk.DefaultECRedundancy
			ft.AddFrom(ioswitch2.NewFromShardstore("22CC59CE3297F78F2D20DC1E33181B77F21E6782097C94E1664F99F129834069", *stgs.Storages[1].MasterHub, stgs.Storages[1].Storage, ioswitch2.SegmentStream(0)))
			ft.AddFrom(ioswitch2.NewFromShardstore("5EAC20EB3EBC7B5FA176C5BD1C01041FB2A6D14C35D6A232CA83D7F1E4B01ADE", *stgs.Storages[1].MasterHub, stgs.Storages[1].Storage, ioswitch2.SegmentStream(1)))
			ft.AddFrom(ioswitch2.NewFromShardstore("A9BC1802F37100C80C72A1D6E8F53C0E0B73F85F99153D8C78FB01CEC9D8D903", *stgs.Storages[1].MasterHub, stgs.Storages[1].Storage, ioswitch2.SegmentStream(2)))
			ft.AddTo(ioswitch2.NewToShardStoreWithRange(*stgs.Storages[0].MasterHub, *stgs.Storages[0], ioswitch2.RawStream(), "raw", exec.NewRange(10, 645)))

			plans := exec.NewPlanBuilder()
			err = parser.Parse(ft, plans)
			if err != nil {
				panic(err)
			}

			fmt.Printf("plans: %v\n", plans)

			exec := plans.Execute(exec.NewExecContext())

			fut := future.NewSetVoid()
			go func() {
				mp, err := exec.Wait(context.Background())
				if err != nil {
					panic(err)
				}

				for k, v := range mp {
					fmt.Printf("%s: %v\n", k, v)
				}

				fut.SetVoid()
			}()

			fut.Wait(context.TODO())
		},
	})
}
