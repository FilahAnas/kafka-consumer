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

	batchSize, err = strconv.Atoi(os.Getenv("BATCH_SIZE"))
	if err != nil || batchSize <= 0 {
		return fmt.Errorf("BATCH_SIZE must be a valid integer greater than 0: %v", err)
	}

	return nil
}

func processMessage(cx context.Context, messages []kafka.Message) error {
	var i int
	for _, message := range messages {	
		if len(message.Value) > 0 {
			var err error
			if i, err = utils.StreamMessageToGCS(gcsBucket, appname, message, i); err != nil {
				fmt.Errorf("failed to stream message to GCS: %w", err)
			}
		} else {
			utils.Warn("Empty message received, skipping...")
		}
	}
	utils.Success("Processed messages successfully:", i)
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
		2, 
		batchSize, 
		2 * time.Minute, 
		ErrorHandler)

}
