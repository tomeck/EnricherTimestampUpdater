package main

import (
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func getKafkaConsumerClient(bootstrapServer string, groupID string, sourceTopic string) (*kafka.Consumer, error) {

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServer,
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	})

	if err == nil {
		consumer.SubscribeTopics([]string{sourceTopic}, nil)
	}

	return consumer, err
}

func getKafkaProducerClient(bootstrapServer string) (*kafka.Producer, error) {

	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrapServer})

	return producer, err
}
