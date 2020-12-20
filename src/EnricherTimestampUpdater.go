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

		// TODO find a better way to wait
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

	// For testing with local Kafka server
	//bootstrapServer := "localhost"
	//groupID := "zed-grp"
	//sourceTopic := "topic1"
	//destTopic := "topic2"

	// Configuration for connecting to IBM Cloud Event Streams instance
	bootstrapServer := "fiser-k-dev-kafka-bootstrap-cp4i.roks-eck-cluster-8a571839bba611238ae425f409ae5396-0000.us-south.containers.appdomain.cloud:443"
	groupID := "zed-grp"
	sourceTopic := "eck-topic1"
	destTopic := "eck-topic2"
	userName := "ibm-iam-bindinfo-platform-auth-idp-credentials-2"
	password := "XCHp0UG7wAVa"

	configMap := &kafka.ConfigMap{
		"bootstrap.servers":                   bootstrapServer,
		"group.id":                            groupID,
		"security.protocol":                   "SASL_SSL",
		"sasl.mechanism":                      "SCRAM-SHA-512",
		"sasl.username":                       userName,
		"sasl.password":                       password,
		"enable.ssl.certificate.verification": false, //  <------------- TODO : THIS IS PROBABLY INSECURE -----------!
		//"ssl.ca.location":   "/usr/local/etc/openssl@1.1/cert.pem",   // This is unnecessary, as it's the default
		"auto.offset.reset": "earliest",
	}

	// FOR TESTING -----------------------

	/*
		// Create a topic
		adminClient, err := getAdminClient(configMap)
		// Create a context for use when calling some of these functions
		// This lets you set a variable timeout on invoking these calls
		// If the timeout passes then an error is returned.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		//var meta kafka.Metadata

		topicResuts, err := adminClient.CreateTopics(
			ctx,
			[]kafka.TopicSpecification{{
				Topic:             sourceTopic,
				NumPartitions:     1,
				ReplicationFactor: 1}},
			kafka.SetAdminOperationTimeout(5000),
		)
		fmt.Printf("metadata: %v\n", topicResuts)

		meta, err := adminClient.GetMetadata(&sourceTopic, false, 1000)
		fmt.Printf("metadata: %v\n", meta)

		defer adminClient.Close()
	*/

	// Produce some messages
	//produce(configMap, sourceTopic)

	// Produce some JSON messages
	produceJSON(configMap, sourceTopic)

	consumer, err := getKafkaConsumerClient(configMap, sourceTopic)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	producer, err := getKafkaProducerClient(configMap)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	// TODO get the consumption loop to exec as a goroutine
	//go consumptionLoop(consumer, producer)
	consumptionLoop(consumer, producer, destTopic)
}
