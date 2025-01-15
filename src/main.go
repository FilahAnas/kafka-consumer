package main

import (
	"context"
	"fmt"
	"os"
	"time"
	"github.com/segmentio/kafka-go"
	"go-kafka-consumer/src/utils"
)

var (
	topic         = os.Getenv("TOPIC")
	brokerAddress = os.Getenv("BROKER_ADDRESS")
	groupID       = os.Getenv("GROUP_ID")
	gcsBucket     = os.Getenv("GCS_BUCKET")
	bqDataset     = os.Getenv("BQ_DATASET")
	bqTable       = os.Getenv("BQ_TABLE")
)

func main() {
	// step 1: Get the Kafka reader
	reader := initKafkaReader(brokerAddress, topic, groupID)
	defer reader.Close()

	// step 2: Read messages from Kafka and stream to GCS
	fmt.Println("Reading Kafka messages ...")
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			fmt.Println("Processing batch to BigQuery")
			// processBatchToBigQuery()
		default:
			message := getMessage(reader)
			fmt.Println("Steaming messages to GCS...")
			if err := utils.StreamMessageToGCS(gcsBucket, message); err != nil {
				fmt.Printf("Failed to stream message to GCS: %v\n", err)
			}
		}
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
		fmt.Printf("Error while reading message: %v\n", err)
	}
	return m
}
