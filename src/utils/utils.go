package utils

import (
	"context"
	"fmt"
	"time"
	"github.com/segmentio/kafka-go"
	"cloud.google.com/go/storage"
	"encoding/json"
)

func StreamMessageToGCS(gcsBucket string, message kafka.Message) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create GCS client: %v", err)
	}
	defer client.Close()

	bucket := client.Bucket(gcsBucket)
	fileName := fmt.Sprintf("kafka_stream_%d.json", time.Now().UnixNano())
	object := bucket.Object(fileName)
	writer := object.NewWriter(ctx)

	jsonMessage, err := json.Marshal(map[string]interface{}{
		"key":   string(message.Key),
		"value": string(message.Value),
		"time":  message.Time,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}

	if _, err := writer.Write(jsonMessage); err != nil {
		return fmt.Errorf("failed to write message to GCS: %v", err)
	}
	_, _ = writer.Write([]byte("\n"))

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close GCS writer: %v", err)
	}

	return nil
}