package main

import (
	"encoding/json"
	"log/slog"
	"toll-calculator/shared"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type DataProducer interface {
	ProduceData(shared.OBUData) error
	Flush(timeoutMS int)
	Close()
}

type KafkaProducer struct {
	producer *kafka.Producer
}

func (p *KafkaProducer) ProduceData(data shared.OBUData) error {
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	err = p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &KafkaTopic, Partition: kafka.PartitionAny},
		Value:          b,
	}, nil)
	if err != nil {
		return err
	}
	return nil
}

func (p *KafkaProducer) Flush(timeoutMS int) {
	p.producer.Flush(timeoutMS)
}

func (p *KafkaProducer) Close() {
	p.producer.Close()
}

func NewKafkaProducer() (*KafkaProducer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		return nil, err
	}

	// handle producer events
	go func() {
		for v := range p.Events() {
			switch ev := v.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					slog.Error("delivery failed", "err", ev.TopicPartition.Error.Error())
				} else {
					slog.Info("delivered message", "topic", *ev.TopicPartition.Topic, "partition", ev.TopicPartition.Partition)
				}
			}
		}
	}()

	return &KafkaProducer{producer: p}, nil
}
