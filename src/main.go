package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
	"sync"
	"github.com/segmentio/kafka-go"
	"go-kafka-consumer/src/utils"
	"github.com/DTSL/golang-libraries/kafkautils"
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
	process       int
	timeout       int
)

func loadEnvVars() error {
	var err error
	envVars := map[string]*string{
		"TOPIC":         &topic,
		"BROKER_ADDRESS": &brokerAddress,
		"GROUP_ID":      &groupID,
		"GCS_BUCKET":    &gcsBucket,
		"BQ_DATASET":    &bqDataset,
		"BQ_TABLE":      &bqTable,
		"PROJECT_ID":    &projectID,
		"APP_NAME":      &appname,
		
	}

	for key, value := range envVars {
		*value = os.Getenv(key)
		if *value == "" {
			return fmt.Errorf("missing environment variable: %s", key)
		}
	}

	process, err = strconv.Atoi(os.Getenv("PROCESS"))
	if err != nil || process <= 0 {
		return fmt.Errorf("PROCESS must be a valid integer greater than 0: %v", err)
	}

	timeout, err = strconv.Atoi(os.Getenv("TIMEOUT"))
	if err != nil || timeout <= 0 {
		return fmt.Errorf("TIMEOUT must be a valid integer greater than 0: %v", err)
	}	

	batchSize, err = strconv.Atoi(os.Getenv("BATCH_SIZE"))
	if err != nil || batchSize <= 0 {
		return fmt.Errorf("BATCH_SIZE must be a valid integer greater than 0: %v", err)
	}

	return nil
}

func processMessage(ctx context.Context, messages []kafka.Message) error {
	var wg sync.WaitGroup
	successCount := 0
	errorCount := 0

	for _, message := range messages {
		wg.Add(1)
		go func(msg kafka.Message) {
			defer wg.Done()
			if len(msg.Value) > 0 {
				if err := utils.StreamMessageToGCS(gcsBucket, appname, msg); err != nil {
					utils.Error("Failed to stream message to GCS", map[string]interface{}{
						"error":  err,
					})	
					errorCount++
				} else {
					successCount++
				}
			} else {
				utils.Warn("Empty message received, skipping...", nil)
			}
		}(message)
	}

	wg.Wait()

	utils.Info("Processed messages successfully", map[string]interface{}{
		"successCount":   successCount,
		"totalMessages": len(messages),
	})	

	return nil
}

func main() {
	utils.Startup()
	ctx := context.Background()

	if err := loadEnvVars(); err != nil {
		log.Fatalf("Configuration error: %v", err)
	}

	readerConfig := kafka.ReaderConfig{
        Brokers:     []string{brokerAddress},
        Topic:       topic,
        GroupID:     groupID,
    }


	ErrorHandler:= func(ctx context.Context, er error){
		log.Fatalf("Error: %v", er)
	}

	kafkautils.RunBatchConsumers(
		ctx, 
		readerConfig, 
		processMessage, 
		process, 
		batchSize, 
		time.Duration(timeout) * time.Minute, 
		ErrorHandler)

}
