package ioswitch2

import (
	"context"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/types"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/common/utils/serder"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	agtrpc "gitlink.org.cn/cloudream/storage/common/pkgs/grpc/agent"
)

var _ = serder.UseTypeUnionExternallyTagged(types.Ref(types.NewTypeUnion[exec.WorkerInfo](
	(*AgentWorker)(nil),
	(*HttpHubWorker)(nil),
)))

type AgentWorker struct {
	Hub     cdssdk.Hub
	Address cdssdk.GRPCAddressInfo
}

func (w *AgentWorker) NewClient() (exec.WorkerClient, error) {
	cli, err := stgglb.AgentRPCPool.Acquire(stgglb.SelectGRPCAddress(w.Hub, w.Address))
	if err != nil {
		return nil, err
	}

	return &AgentWorkerClient{hubID: w.Hub.HubID, cli: cli}, nil
}

func (w *AgentWorker) String() string {
	return w.Hub.String()
}

func (w *AgentWorker) Equals(worker exec.WorkerInfo) bool {
	aw, ok := worker.(*AgentWorker)
	if !ok {
		return false
	}

	return w.Hub.HubID == aw.Hub.HubID
}

type AgentWorkerClient struct {
	hubID cdssdk.HubID
	cli   *agtrpc.PoolClient
}

func (c *AgentWorkerClient) ExecutePlan(ctx context.Context, plan exec.Plan) error {
	return c.cli.ExecuteIOPlan(ctx, plan)
}
func (c *AgentWorkerClient) SendStream(ctx context.Context, planID exec.PlanID, id exec.VarID, stream io.ReadCloser) error {
	return c.cli.SendStream(ctx, planID, id, io2.CounterCloser(stream, func(cnt int64, err error) {
		if stgglb.Stats.HubTransfer != nil {
			stgglb.Stats.HubTransfer.RecordOutput(c.hubID, cnt, err == nil || err == io.EOF)
		}
	}))
}
func (c *AgentWorkerClient) SendVar(ctx context.Context, planID exec.PlanID, id exec.VarID, value exec.VarValue) error {
	return c.cli.SendVar(ctx, planID, id, value)
}
func (c *AgentWorkerClient) GetStream(ctx context.Context, planID exec.PlanID, streamID exec.VarID, signalID exec.VarID, signal exec.VarValue) (io.ReadCloser, error) {
	str, err := c.cli.GetStream(ctx, planID, streamID, signalID, signal)
	if err != nil {
		return nil, err
	}

	return io2.CounterCloser(str, func(cnt int64, err error) {
		if stgglb.Stats.HubTransfer != nil {
			stgglb.Stats.HubTransfer.RecordInput(c.hubID, cnt, err == nil || err == io.EOF)
		}
	}), nil
}
func (c *AgentWorkerClient) GetVar(ctx context.Context, planID exec.PlanID, varID exec.VarID, signalID exec.VarID, signal exec.VarValue) (exec.VarValue, error) {
	return c.cli.GetVar(ctx, planID, varID, signalID, signal)
}
func (c *AgentWorkerClient) Close() error {
	stgglb.AgentRPCPool.Release(c.cli)
	return nil
}
