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
    "os/signal"
    "syscall"
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
	appname       string
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
	appname = os.Getenv("APP_NAME")

	if topic == "" || brokerAddress == "" || groupID == "" || gcsBucket == "" || bqDataset == "" || bqTable == "" || projectID == "" {
		return fmt.Errorf("missing one or more environment variables")
	}

	batchSize, err = strconv.Atoi(os.Getenv("BATCH_SIZE"))
	if err != nil || batchSize <= 0 {
		return fmt.Errorf("BATCH_SIZE must be a valid integer greater than 0: %v", err)
	}

	return nil
}

func processBatch() {
	if err := utils.ProcessBatchToBigQuery(projectID, gcsBucket, appname, bqDataset, bqTable); err != nil {
		log.Printf("Failed to process batch to BigQuery: %v", err)
	}
}

func processMessage(reader *kafka.Reader, appname string) error {

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	message, err := reader.ReadMessage(ctx)
	if err != nil {
		return fmt.Errorf("error reading message: %w", err)
	}

	if len(message.Value) > 0 {
		if err := utils.StreamMessageToGCS(gcsBucket, appname, message); err != nil {
			return fmt.Errorf("failed to stream message to GCS: %w", err)
		}
	} else {
		utils.Warn("Empty message received, skipping...")
	}
	return nil
}

func main() {
	utils.Startup()

	if err := loadEnvVars(); err != nil {
		log.Fatalf("Configuration error: %v", err)
	}

	reader := utils.InitKafkaReader(brokerAddress, topic, groupID)
	defer reader.Close()

	RunConsumer(processBatch, processMessage, reader, appname, batchSize, 10*time.Minute)
}


func RunConsumer(processBatch func(), processMessage func(*kafka.Reader, string) error, reader *kafka.Reader, appname string, batchSize int, timeout time.Duration) {
    ticker := time.NewTicker(timeout)
    defer ticker.Stop()
    messageCount := 0

    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {

		case <-ticker.C:
			if messageCount > 0 {
				processBatch()
				messageCount = 0
			}

		case sig := <-sigChan:
			utils.Warn("Received signal: %s. Shutting down ...\n", sig)
			if messageCount > 0 {
				processBatch()
			}
			if err := reader.Close(); err != nil {
				utils.Error("Failed to close Kafka reader: %v\n", err)
			}
			return

		default:
			if err := processMessage(reader, appname); err == nil {
				messageCount++
			}
			if messageCount >= batchSize {
				processBatch()
				messageCount = 0
			}
		}
	}
}
