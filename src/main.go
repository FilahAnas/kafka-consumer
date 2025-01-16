package main

import (
	"context"
	"fmt"
	"os"
	"time"
	"github.com/segmentio/kafka-go"
	"go-kafka-consumer/src/utils"
	"strconv"
)

var (
	topic         = os.Getenv("TOPIC")
	brokerAddress = os.Getenv("BROKER_ADDRESS")
	groupID       = os.Getenv("GROUP_ID")
	gcsBucket     = os.Getenv("GCS_BUCKET")
	bqDataset     = os.Getenv("BQ_DATASET")
	bqTable       = os.Getenv("BQ_TABLE")
	batchSize, err      = strconv.Atoi(os.Getenv("BATCH_SIZE"))
	projectID       = os.Getenv("PROJECT_ID")
)
func main() {
	// step 1: Get the Kafka reader
	reader := initKafkaReader(brokerAddress, topic, groupID)
	defer reader.Close()

	// step 2: Read messages from Kafka and stream to GCS
	fmt.Println("Reading Kafka messages ...")
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	i := 0
	for {
		select {
		case <-ticker.C:
			if i >= batchSize {
				processBatch()
			}
		default:
			processMessage(reader)
			i++

		}
	}
}

func processBatch() {
	fmt.Println("Processing batch to BigQuery")
	if err := utils.ProcessBatchToBigQuery(projectID, gcsBucket, bqDataset, bqTable); err != nil {
		fmt.Printf("Failed to Batch messages to BQ: %v\n", err)
	}
}

func processMessage(reader *kafka.Reader) {
	message := getMessage(reader)
	if len(message.Value) > 0 {
		fmt.Println("Streaming messages to GCS...")
		if err := utils.StreamMessageToGCS(gcsBucket, message); err != nil {
			fmt.Printf("Failed to stream message to GCS: %v\n", err)
		}
	} else {
		fmt.Println("Received empty message, skipping...")
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
