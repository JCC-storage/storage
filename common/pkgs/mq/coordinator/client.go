package coordinator

import (
	"sync"

	"gitlink.org.cn/cloudream/common/pkgs/mq"
	stgmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq"
)

type Client struct {
	rabbitCli *mq.RabbitMQTransport
}

func NewClient(cfg mq.Config) (*Client, error) {
	rabbitCli, err := mq.NewRabbitMQTransport(cfg, stgmq.COORDINATOR_QUEUE_NAME, "")
	if err != nil {
		return nil, err
	}

	return &Client{
		rabbitCli: rabbitCli,
	}, nil
}

func (c *Client) Close() {
	c.rabbitCli.Close()
}

type Pool interface {
	Acquire() (*Client, error)
	Release(cli *Client)
}

type pool struct {
	mqcfg  mq.Config
	shared *Client
	lock   sync.Mutex
}

func NewPool(mqcfg mq.Config) Pool {
	return &pool{
		mqcfg: mqcfg,
	}
}
func (p *pool) Acquire() (*Client, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.shared == nil {
		var err error
		p.shared, err = NewClient(p.mqcfg)
		if err != nil {
			return nil, err
		}
	}

	return p.shared, nil
}

func (p *pool) Release(cli *Client) {
}
