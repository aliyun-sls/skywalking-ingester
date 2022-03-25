package receiver

import (
	"github.com/aliyun-sls/skywalking-ingester/configure"
	"github.com/aliyun-sls/skywalking-ingester/modules"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Receiver interface {
	ReceiveData() (modules.OriginData, error)
}

func NewReceiver(config configure.Configuration) (Receiver, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  config.BootstrapServers(),
		"group.id":           config.GroupID(),
		"session.timeout.ms": 6000,
		"auto.offset.reset":  "latest",
	})

	if err != nil {
		return nil, err
	}

	if e := c.SubscribeTopics(config.Topics(), nil); e != nil {
		return nil, err
	}

	return &KafkaReceiver{consumer: c, config: config}, nil
}

type KafkaReceiver struct {
	consumer *kafka.Consumer
	config   configure.Configuration
}

func (r *KafkaReceiver) ReceiveData() (modules.OriginData, error) {
	ev := r.consumer.Poll(1000)
	if ev == nil {
		return nil, nil
	}

	switch e := ev.(type) {
	case *kafka.Message:
		return modules.NewOriginData(r.config, *e.TopicPartition.Topic, e.Value), nil
	case *kafka.Error:
		return nil, e
	default:
		return nil, nil
	}

}
