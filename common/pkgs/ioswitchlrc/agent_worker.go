package ioswitchlrc

import (
	"context"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	agtrpc "gitlink.org.cn/cloudream/storage/common/pkgs/grpc/agent"
)

// var _ = serder.UseTypeUnionExternallyTagged(types.Ref(types.NewTypeUnion[exec.WorkerInfo](
// 	(*AgentWorker)(nil),
// )))

type AgentWorker struct {
	Node    cdssdk.Node
	Address cdssdk.GRPCAddressInfo
}

func (w *AgentWorker) NewClient() (exec.WorkerClient, error) {
	cli, err := stgglb.AgentRPCPool.Acquire(stgglb.SelectGRPCAddress(w.Node, w.Address))
	if err != nil {
		return nil, err
	}

	return &AgentWorkerClient{cli: cli}, nil
}

func (w *AgentWorker) String() string {
	return w.Node.String()
}

func (w *AgentWorker) Equals(worker exec.WorkerInfo) bool {
	aw, ok := worker.(*AgentWorker)
	if !ok {
		return false
	}

	return w.Node.NodeID == aw.Node.NodeID
}

type AgentWorkerClient struct {
	cli *agtrpc.PoolClient
}

func (c *AgentWorkerClient) ExecutePlan(ctx context.Context, plan exec.Plan) error {
	return c.cli.ExecuteIOPlan(ctx, plan)
}
func (c *AgentWorkerClient) SendStream(ctx context.Context, planID exec.PlanID, id exec.VarID, stream io.ReadCloser) error {
	return c.cli.SendStream(ctx, planID, id, stream)
}
func (c *AgentWorkerClient) SendVar(ctx context.Context, planID exec.PlanID, id exec.VarID, value exec.VarValue) error {
	return c.cli.SendVar(ctx, planID, id, value)
}
func (c *AgentWorkerClient) GetStream(ctx context.Context, planID exec.PlanID, streamID exec.VarID, signalID exec.VarID, signal exec.VarValue) (io.ReadCloser, error) {
	return c.cli.GetStream(ctx, planID, streamID, signalID, signal)
}
func (c *AgentWorkerClient) GetVar(ctx context.Context, planID exec.PlanID, varID exec.VarID, signalID exec.VarID, signal exec.VarValue) (exec.VarValue, error) {
	return c.cli.GetVar(ctx, planID, varID, signalID, signal)
}
func (c *AgentWorkerClient) Close() error {
	stgglb.AgentRPCPool.Release(c.cli)
	return nil
}
