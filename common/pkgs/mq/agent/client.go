package agent

import (
	"sync"

	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq"
)

type Client struct {
	rabbitCli *mq.RabbitMQTransport
	id        cdssdk.HubID
}

func NewClient(id cdssdk.HubID, cfg *stgmq.Config) (*Client, error) {
	rabbitCli, err := mq.NewRabbitMQTransport(cfg.MakeConnectingURL(), stgmq.MakeAgentQueueName(int64(id)), "")
	if err != nil {
		return nil, err
	}

	return &Client{
		rabbitCli: rabbitCli,
		id:        id,
	}, nil
}

func (c *Client) Close() {
	c.rabbitCli.Close()
}

type Pool interface {
	Acquire(id cdssdk.HubID) (*Client, error)
	Release(cli *Client)
}

type pool struct {
	mqcfg   *stgmq.Config
	shareds map[cdssdk.HubID]*Client
	lock    sync.Mutex
}

func NewPool(mqcfg *stgmq.Config) Pool {
	return &pool{
		mqcfg:   mqcfg,
		shareds: make(map[cdssdk.HubID]*Client),
	}
}
func (p *pool) Acquire(id cdssdk.HubID) (*Client, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	cli, ok := p.shareds[id]
	if !ok {
		var err error
		cli, err = NewClient(id, p.mqcfg)
		if err != nil {
			return nil, err
		}
		p.shareds[id] = cli
	}

	return cli, nil
}

func (p *pool) Release(cli *Client) {
	// TODO 定时关闭
}
