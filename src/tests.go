package main

import "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"

// FOR TESTING
func produce(configMap *kafka.ConfigMap, topic string) {

	producer, err := getKafkaProducerClient(configMap)
	if err != nil {
		panic(err)
	}

	// Produce messages to topic (asynchronously)
	for _, word := range []string{"Welcome", "to", "the", "Confluent", "Kafka", "Golang", "client"} {
		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(word),
		}, nil)
	}

	producer.Flush(5000)
	producer.Close()
}

func produceJSON(configMap *kafka.ConfigMap, topic string) {

	producer, err := getKafkaProducerClient(configMap)
	if err != nil {
		panic(err)
	}

	someJson := "{\"Name\":\"Tom\", \"Age\":44}"

	// Produce messages to topic (asynchronously)
	for i := 1; i < 10; i++ {
		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(someJson),
		}, nil)
	}

	producer.Flush(5000)
	producer.Close()
}
