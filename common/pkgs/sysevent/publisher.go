package sysevent

import (
	"fmt"
	"time"

	"github.com/streadway/amqp"
	"gitlink.org.cn/cloudream/common/pkgs/async"
	"gitlink.org.cn/cloudream/common/utils/serder"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
)

type PublisherEvent interface{}

type PublisherExited struct {
	Err error
}

type PublishError struct {
	Err error
}

type OtherError struct {
	Err error
}

type Publisher struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	eventChan  *async.UnboundChannel[SysEvent]
	thisSource Source
}

func NewPublisher(cfg Config, thisSource Source) (*Publisher, error) {
	config := amqp.Config{
		Vhost: cfg.VHost,
	}

	url := fmt.Sprintf("amqp://%s:%s@%s", cfg.Account, cfg.Password, cfg.Address)
	connection, err := amqp.DialConfig(url, config)
	if err != nil {
		return nil, err
	}

	channel, err := connection.Channel()
	if err != nil {
		connection.Close()
		return nil, fmt.Errorf("openning channel on connection: %w", err)
	}

	err = channel.ExchangeDeclare(ExchangeName, "fanout", false, true, false, false, nil)
	if err != nil {
		connection.Close()
		return nil, fmt.Errorf("declare exchange: %w", err)
	}

	pub := &Publisher{
		connection: connection,
		channel:    channel,
		eventChan:  async.NewUnboundChannel[SysEvent](),
		thisSource: thisSource,
	}

	return pub, nil
}

func (p *Publisher) Start() *async.UnboundChannel[PublisherEvent] {
	ch := async.NewUnboundChannel[PublisherEvent]()
	go func() {
		defer ch.Close()
		defer p.channel.Close()
		defer p.connection.Close()

		for {
			event := <-p.eventChan.Receive().Chan()
			if event.Err != nil {
				if event.Err == async.ErrChannelClosed {
					ch.Send(PublisherExited{Err: nil})
				} else {
					ch.Send(PublisherExited{Err: event.Err})
				}
				return
			}

			eventData, err := serder.ObjectToJSONEx(event.Value)
			if err != nil {
				ch.Send(OtherError{Err: fmt.Errorf("serialize event data: %w", err)})
				continue
			}

			err = p.channel.Publish(ExchangeName, "", false, false, amqp.Publishing{
				ContentType: "text/plain",
				Body:        eventData,
				Expiration:  "60000", // 消息超时时间默认1分钟
			})
			if err != nil {
				ch.Send(PublishError{Err: err})
				continue
			}
		}
	}()

	return ch
}

// Publish 发布事件，会自动补齐必要信息
func (p *Publisher) Publish(eventBody stgmod.SysEventBody) {
	p.eventChan.Send(stgmod.SysEvent{
		Timestamp: time.Now(),
		Source:    p.thisSource,
		Body:      eventBody,
	})
}

// PublishRaw 完全原样发布事件，不补齐任何信息
func (p *Publisher) PublishRaw(evt SysEvent) {
	p.eventChan.Send(evt)
}
