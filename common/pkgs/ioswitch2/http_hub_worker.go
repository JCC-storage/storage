package ioswitch2

import (
	"context"
	"io"
	"strconv"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/types"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/sdks/storage/cdsapi"
	"gitlink.org.cn/cloudream/common/utils/serder"
)

var _ = serder.UseTypeUnionExternallyTagged(types.Ref(types.NewTypeUnion[exec.WorkerInfo](
	(*HttpHubWorker)(nil),
)))

type HttpHubWorker struct {
	Node cdssdk.Node
}

func (w *HttpHubWorker) NewClient() (exec.WorkerClient, error) {
	addressInfo := w.Node.Address.(*cdssdk.HttpAddressInfo)
	baseUrl := "http://" + addressInfo.ExternalIP + ":" + strconv.Itoa(addressInfo.Port)
	config := cdsapi.Config{
		URL: baseUrl,
	}
	pool := cdsapi.NewPool(&config)
	cli, err := pool.Acquire()
	defer pool.Release(cli)
	if err != nil {
		return nil, err
	}

	return &HttpHubWorkerClient{cli: cli}, nil
}

func (w *HttpHubWorker) String() string {
	return w.Node.String()
}

func (w *HttpHubWorker) Equals(worker exec.WorkerInfo) bool {
	aw, ok := worker.(*HttpHubWorker)
	if !ok {
		return false
	}

	return w.Node.NodeID == aw.Node.NodeID
}

type HttpHubWorkerClient struct {
	cli *cdsapi.Client
}

func (c *HttpHubWorkerClient) ExecutePlan(ctx context.Context, plan exec.Plan) error {
	return c.cli.ExecuteIOPlan(cdsapi.ExecuteIOPlanReq{
		Plan: plan,
	})
}
func (c *HttpHubWorkerClient) SendStream(ctx context.Context, planID exec.PlanID, id exec.VarID, stream io.ReadCloser) error {
	return c.cli.SendStream(cdsapi.SendStreamReq{
		SendStreamInfo: cdsapi.SendStreamInfo{
			PlanID: planID,
			VarID:  id,
		},
		Stream: stream,
	})
}
func (c *HttpHubWorkerClient) SendVar(ctx context.Context, planID exec.PlanID, id exec.VarID, value exec.VarValue) error {
	return c.cli.SendVar(cdsapi.SendVarReq{
		PlanID:   planID,
		VarID:    id,
		VarValue: value,
	})
}
func (c *HttpHubWorkerClient) GetStream(ctx context.Context, planID exec.PlanID, streamID exec.VarID, signalID exec.VarID, signal exec.VarValue) (io.ReadCloser, error) {
	return c.cli.GetStream(cdsapi.GetStreamReq{
		PlanID:   planID,
		VarID:    streamID,
		SignalID: signalID,
		Signal:   signal,
	})
}
func (c *HttpHubWorkerClient) GetVar(ctx context.Context, planID exec.PlanID, varID exec.VarID, signalID exec.VarID, signal exec.VarValue) (exec.VarValue, error) {
	resp, err := c.cli.GetVar(cdsapi.GetVarReq{
		PlanID:   planID,
		VarID:    varID,
		SignalID: signalID,
		Signal:   signal,
	})
	if err != nil {
		return nil, err
	}

	return resp.Value, err
}
func (c *HttpHubWorkerClient) Close() error {
	//stgglb.AgentRPCPool.Release(c.cli)
	return nil
}
