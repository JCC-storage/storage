package mq

import (
	"fmt"
	jsoniter "github.com/json-iterator/go"
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
		"datamap_hubinfo",
		"datamap_storageinfo",
		"datamap_storagestats",
		"datamap_hubtransferstats",
		"datamap_hubstoragetransferstats",
		"datamap_blocktransfer",
		"datamap_blockdistribution",
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
	case "datamap_hubinfo":
		var data stgmod.HubInfo
		if err := jsoniter.Unmarshal(body, &data); err != nil {
			log.Printf("Failed to unmarshal HubInfo: %v, body: %s", err, body)
			return
		}
		models.ProcessHubInfo(data)
	case "datamap_storageinfo":
		var data stgmod.StorageInfo
		if err := jsoniter.Unmarshal(body, &data); err != nil {
			log.Printf("Failed to unmarshal StorageInfo: %v, body: %s", err, body)
			return
		}
		models.ProcessStorageInfo(data)
	case "datamap_storagestats":
		var data stgmod.StorageStats
		if err := jsoniter.Unmarshal(body, &data); err != nil {
			log.Printf("Failed to unmarshal StorageStats: %v, body: %s", err, body)
			return
		}
		//models.ProcessStorageInfo(data)
	case "datamap_hubtransferstats":
		var data stgmod.HubTransferStats
		err := jsoniter.Unmarshal(body, &data)
		if err != nil {
			log.Printf("Failed to unmarshal HubTransferStats: %v, body: %s", err, body)
			return
		}
		models.ProcessHubTransfer(data)
	case "datamap_hubstoragetransferstats":
		var data stgmod.HubStorageTransferStats
		err := jsoniter.Unmarshal(body, &data)
		if err != nil {
			log.Printf("Failed to unmarshal HubStorageTransferStats: %v, body: %s", err, body)
			return
		}
		//models.ProcessHubTransfer(data)
	case "datamap_blocktransfer":
		var data stgmod.BlockTransfer
		err := jsoniter.Unmarshal(body, &data)
		if err != nil {
			log.Printf("Failed to unmarshal BlockTransfer: %v, body: %s", err, body)
			return
		}
		models.ProcessBlockTransfer(data)
	case "datamap_blockdistribution":
		var data stgmod.BlockDistribution
		err := jsoniter.Unmarshal(body, &data)
		if err != nil {
			log.Printf("Failed to unmarshal BlockDistribution: %v, body: %s", err, body)
			return
		}
		models.ProcessBlockDistribution(data)
	default:
		log.Printf("Unknown queue: %s", queue)
	}
}
