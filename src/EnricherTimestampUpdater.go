package main

import (
	"encoding/json"
	"fmt"
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func processMessage(msg *kafka.Message, producer *kafka.Producer, nextTopic string) {

	fmt.Printf("ProcessMessage is processing message: %s\n", msg.Value)

	var jsonMap map[string]interface{}
	json.Unmarshal([]byte(msg.Value), &jsonMap)

	// TODO handle malformed JSON (jsonMap will be nil)
	if jsonMap == nil {
		fmt.Printf("ERROR: Invalid JSON format\n")
	} else {
		fmt.Printf("ProcessMessage decoded: Name=%s, Age=%.0f\n", jsonMap["Name"], jsonMap["Age"])

		// Add a value
		jsonMap["updatedTimestamp"] = time.Now().Unix()

		fmt.Printf("ProcessMessage added: updatedTimestamp=%d\n", jsonMap["updatedTimestamp"])

		// Get our map as a string
		jsonString, _ := json.Marshal(jsonMap)

		// Now publish back out to next topic
		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &nextTopic, Partition: kafka.PartitionAny},
			Value:          []byte(jsonString),
		}, nil)

		// Wait for message deliveries before shutting down
		producer.Flush(2 * 1000)
	}
}

func consumptionLoop(consumer *kafka.Consumer, producer *kafka.Producer, destTopic string) {
	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			//fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
			processMessage(msg, producer, destTopic)
		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v\n", err)
		}
	}
}

func main() {
	bootstrapServer := "localhost"
	groupID := "zed-grp"
	sourceTopic := "topic1"
	destTopic := "topic2"

	// FOR TESTING
	// Produce some messages
	//go produce(bootstrapServer, sourceTopic)

	consumer, err := getKafkaConsumerClient(bootstrapServer, groupID, sourceTopic)
	if err != nil {
		panic(err)
	}

	producer, err := getKafkaProducerClient(bootstrapServer)
	if err != nil {
		panic(err)
	}

	// TODO confirm that this works
	defer consumer.Close()
	defer producer.Close()

	// TODO get the consumption loop to exec as a goroutine
	//go consumptionLoop(consumer, producer)
	consumptionLoop(consumer, producer, destTopic)
}
