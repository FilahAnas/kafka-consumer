package main

import (
	"context"
	"fmt"
	"os"
	"github.com/segmentio/kafka-go"
)

var (
	topic         = os.Getenv("TOPIC")
	brokerAddress = os.Getenv("BROKER_ADDRESS")
	groupID       = os.Getenv("GROUP_ID")
)

func main() {
	// Get the Kafka reader
	reader := initKafkaReader(brokerAddress, topic, groupID)
	defer reader.Close()

	fmt.Println("Reading Kafka messages ...")
	// Read messages from Kafka
	for {
		message := getMessage(reader)
		fmt.Printf("Message :  %s\n", message.Value)
	}
}

func initKafkaReader(brokerAddress, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{brokerAddress},
		Topic:       topic,
		GroupID:     groupID,
		MinBytes:    10e3,
		MaxBytes:    10e6,
		StartOffset: kafka.LastOffset,
	})
}

func getMessage(reader *kafka.Reader) kafka.Message {
	m, err := reader.ReadMessage(context.Background())
	if err != nil {
		fmt.Printf("Error while reading message : %v\n", err)
	}
	return m
}