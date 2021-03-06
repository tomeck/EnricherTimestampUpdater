package main

import (
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func getKafkaConsumerClient(configMap *kafka.ConfigMap, sourceTopic string) (*kafka.Consumer, error) {

	consumer, err := kafka.NewConsumer(configMap)

	if err == nil {
		consumer.SubscribeTopics([]string{sourceTopic}, nil)
	}

	return consumer, err
}

/*
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
*/

func getKafkaProducerClient(configMap *kafka.ConfigMap) (*kafka.Producer, error) {

	producer, err := kafka.NewProducer(configMap)

	return producer, err
}

func getAdminClient(configMap *kafka.ConfigMap) (*kafka.AdminClient, error) {
	// Variable p holds the new AdminClient instance.
	admin, err := kafka.NewAdminClient(configMap)

	return admin, err
}
