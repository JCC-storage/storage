package mq

import (
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/datamap/internal/config"
	"gitlink.org.cn/cloudream/storage/datamap/internal/models"
	"log"
)

func InitMQ(cfg config.RabbitMQConfig) (*amqp.Connection, error) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/",
		cfg.User, cfg.Password, cfg.Host, cfg.Port))
	if err != nil {
		return nil, err
	}

	// 启动队列监听
	go listenQueues(conn)

	return conn, nil
}

func listenQueues(conn *amqp.Connection) {
	queues := []string{
		"datamap_storageinfo",
		"datamap_hubtransfer",
		"datamap_blocktransfer",
		"datamap_blockdistribution",
		"datamap_objectchange",
		"datamap_packagechange",
		"datamap_bucketchange",
	}

	for _, queue := range queues {
		go func(q string) {
			ch, err := conn.Channel()
			if err != nil {
				log.Printf("Failed to open channel for queue %s: %v", q, err)
				return
			}
			defer ch.Close()

			msgs, err := ch.Consume(q, "", true, false, false, false, nil)
			if err != nil {
				log.Printf("Failed to register consumer for queue %s: %v", q, err)
				return
			}

			for msg := range msgs {
				processMessage(q, msg.Body)
			}
		}(queue)
	}
}

func processMessage(queue string, body []byte) {
	switch queue {
	case "datamap_storageinfo":
		var data stgmod.HubStat
		json.Unmarshal(body, &data)
		models.ProcessHubStat(data)
	case "datamap_hubtransfer":
		var data stgmod.HubTrans
		json.Unmarshal(body, &data)
		models.ProcessHubTrans(data)
	case "datamap_blocktransfer":
		var data stgmod.Object
		json.Unmarshal(body, &data)
		models.ProcessBlockTransInfo(data)
	case "datamap_blockdistribution":
		var data stgmod.BlockTransInfo
		json.Unmarshal(body, &data)
		models.ProcessBlockDistributionInfo(data)
	default:
		log.Printf("Unknown queue: %s", queue)
	}
}
