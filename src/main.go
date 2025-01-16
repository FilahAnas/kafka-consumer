package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"go-kafka-consumer/src/utils"
)

var (
	topic         string
	brokerAddress string
	groupID       string
	gcsBucket     string
	bqDataset     string
	bqTable       string
	batchSize     int
	projectID     string
)

func loadEnvVars() error {
	var err error
	topic = os.Getenv("TOPIC")
	brokerAddress = os.Getenv("BROKER_ADDRESS")
	groupID = os.Getenv("GROUP_ID")
	gcsBucket = os.Getenv("GCS_BUCKET")
	bqDataset = os.Getenv("BQ_DATASET")
	bqTable = os.Getenv("BQ_TABLE")
	projectID = os.Getenv("PROJECT_ID")

	if topic == "" || brokerAddress == "" || groupID == "" || gcsBucket == "" || bqDataset == "" || bqTable == "" || projectID == "" {
		return fmt.Errorf("missing one or more environment variables")
	}

	batchSize, err = strconv.Atoi(os.Getenv("BATCH_SIZE"))
	if err != nil || batchSize <= 0 {
		return fmt.Errorf("BATCH_SIZE must be a valid integer greater than 0: %v", err)
	}

	return nil
}



func initKafkaReader() *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{brokerAddress},
		Topic:       topic,
		GroupID:     groupID,
		MinBytes:    10e3,  // 10KB
		MaxBytes:    10e6,  // 10MB
		StartOffset: kafka.LastOffset,
	})
}


func processBatch() {
	log.Println("Processing batch to BigQuery...")
	if err := utils.ProcessBatchToBigQuery(projectID, gcsBucket, bqDataset, bqTable); err != nil {
		log.Printf("Failed to process batch to BigQuery: %v", err)
	}
}

func processMessage(reader *kafka.Reader) error {
	message, err := reader.ReadMessage(context.Background())
	log.Println("Reading message...")
	if err != nil {
		return fmt.Errorf("error reading message: %w", err)
	}

	if len(message.Value) > 0 {
		log.Println("Streaming message to GCS...")
		if err := utils.StreamMessageToGCS(gcsBucket, message); err != nil {
			return fmt.Errorf("failed to stream message to GCS: %w", err)
		}
	} else {
		log.Println("Empty message received, skipping...")
	}

	return nil
}



func main() {
	if err := loadEnvVars(); err != nil {
		log.Fatalf("Configuration error: %v", err)
	}

	reader := initKafkaReader()
	defer reader.Close()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	i := 0
	for {
		select {
		case <-ticker.C:
			if i >= batchSize {
				processBatch()
				i = 0
			}
		default:
			if err := processMessage(reader); err != nil {
				log.Printf("Error processing message: %v", err)
			} else {
				i++
			}
		}
	}}
