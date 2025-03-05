package sysevent

import (
	"fmt"
	"sync"

	"github.com/streadway/amqp"
	"gitlink.org.cn/cloudream/common/pkgs/async"
	"gitlink.org.cn/cloudream/common/utils/lo2"
	"gitlink.org.cn/cloudream/common/utils/serder"
)

type Watcher interface {
	OnEvent(event SysEvent)
}

type WatcherEvent interface{}

type WatcherExited struct {
	Err error
}

type WatcherHost struct {
	watchers   []Watcher
	lock       sync.Mutex
	connection *amqp.Connection
	channel    *amqp.Channel
	recvChan   <-chan amqp.Delivery
}

func NewWatcherHost(cfg Config) (*WatcherHost, error) {
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

	_, err = channel.QueueDeclare(
		SysEventQueueName,
		false,
		true,
		false,
		false,
		nil,
	)
	if err != nil {
		channel.Close()
		connection.Close()
		return nil, fmt.Errorf("declare queue: %w", err)
	}

	err = channel.QueueBind(SysEventQueueName, "", ExchangeName, false, nil)
	if err != nil {
		channel.Close()
		connection.Close()
		return nil, fmt.Errorf("bind queue: %w", err)
	}

	recvChan, err := channel.Consume(SysEventQueueName, "", true, false, true, false, nil)
	if err != nil {
		channel.Close()
		connection.Close()
		return nil, fmt.Errorf("consume queue: %w", err)
	}

	wat := &WatcherHost{
		connection: connection,
		channel:    channel,
		recvChan:   recvChan,
	}

	return wat, nil
}

func (w *WatcherHost) Start() *async.UnboundChannel[WatcherEvent] {
	ch := async.NewUnboundChannel[WatcherEvent]()

	go func() {
		defer ch.Close()
		defer w.channel.Close()
		defer w.connection.Close()

		for m := range w.recvChan {
			evt, err := serder.JSONToObjectEx[SysEvent](m.Body)
			if err != nil {
				ch.Send(OtherError{Err: fmt.Errorf("deserialize event: %w", err)})
				continue
			}

			w.lock.Lock()
			ws := make([]Watcher, 0, len(w.watchers))
			ws = append(ws, w.watchers...)
			w.lock.Unlock()

			for _, w := range ws {
				w.OnEvent(evt)
			}
		}

		ch.Send(WatcherExited{Err: nil})
	}()

	return ch
}

func (w *WatcherHost) AddWatcher(watcher Watcher) {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.watchers = append(w.watchers, watcher)
}

func (w *WatcherHost) AddWatcherFn(fn func(event SysEvent)) Watcher {
	watcher := &fnWatcher{fn: fn}
	w.AddWatcher(watcher)
	return watcher
}

func (w *WatcherHost) RemoveWatcher(watcher Watcher) {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.watchers = lo2.Remove(w.watchers, watcher)
}

type fnWatcher struct {
	fn func(event SysEvent)
}

func (w *fnWatcher) OnEvent(event SysEvent) {
	w.fn(event)
}
