package utils

import (
    "context"
    "fmt"
    "time"
    "github.com/segmentio/kafka-go"
    "cloud.google.com/go/storage"
    "encoding/json"
)
func StreamMessageToGCS(gcsBucket string, appname string, message kafka.Message) (error) {
    ctx := context.Background()
    client, err := storage.NewClient(ctx)
    if err != nil {
        return fmt.Errorf("failed to create GCS client: %v", err)
    }
    defer client.Close()

    bucket := client.Bucket(gcsBucket)
    fileName := fmt.Sprintf("%s/kafka_stream_%d.json", appname, time.Now().UnixNano())
    object := bucket.Object(fileName)
    writer := object.NewWriter(ctx)

    var msgData map[string]interface{}
    if err := json.Unmarshal(message.Value, &msgData); err != nil {
        return err
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
