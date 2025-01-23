package utils

import (
    "context"
    "fmt"
    "time"
    "github.com/segmentio/kafka-go"
    "cloud.google.com/go/storage"
    "encoding/json"
)
func StreamMessageToGCS(gcsBucket string, appname string, message kafka.Message, i int) (int, error) {
    ctx := context.Background()
    client, err := storage.NewClient(ctx)
    if err != nil {
        Error("Failed to create GCS client:", err)
        return i, fmt.Errorf("failed to create GCS client: %v", err)
    }
    defer client.Close()

    bucket := client.Bucket(gcsBucket)
    fileName := fmt.Sprintf("%s/kafka_stream_%d.json", appname, time.Now().UnixNano())
    object := bucket.Object(fileName)
    writer := object.NewWriter(ctx)

    var msgData map[string]interface{}
    if err := json.Unmarshal(message.Value, &msgData); err != nil {
        Error("Failed to unmarshal message value:", err)
        return i, err
    }
    currentTime := time.Now().UTC()
    formattedTime := currentTime.Format("2006-01-02 15:04:05 UTC")

    jsonMessage, err := json.Marshal(map[string]interface{}{
        "inserted_at": formattedTime,
        "app_name": msgData["appname"],
        "payload":    msgData["content"],
        "extracted_at": msgData["datetime"],
    })
    if err != nil {
        Error("Failed to marshal message:", err)
        return i, fmt.Errorf("failed to marshal message: %v", err)
    }

    if _, err := writer.Write(jsonMessage); err != nil {
        Error("Failed to write message to GCS:", err)
        return i, fmt.Errorf("failed to write message to GCS: %v", err)
    }
    _, _ = writer.Write([]byte("\n"))

    if err := writer.Close(); err != nil {
        Error("Failed to close GCS writer:", err)
        return i, fmt.Errorf("failed to close GCS writer: %v", err)
    }
    i++
    return i, nil
}
