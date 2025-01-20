package utils

import (
    "context"
    "fmt"
    "time"
    "github.com/segmentio/kafka-go"
    "cloud.google.com/go/storage"
    "cloud.google.com/go/bigquery"
    "encoding/json"
    "google.golang.org/api/iterator"
)

func InitKafkaReader(brokerAddress string, topic string, groupID string) *kafka.Reader {
    return kafka.NewReader(kafka.ReaderConfig{
        Brokers:     []string{brokerAddress},
        Topic:       topic,
        GroupID:     groupID,
        MinBytes:    10e3,  // 10KB
        MaxBytes:    10e6,  // 10MB
        StartOffset: kafka.LastOffset,
    })
}

func StreamMessageToGCS(gcsBucket string, appname string, message kafka.Message) error {
    ctx := context.Background()
    client, err := storage.NewClient(ctx)
    if err != nil {
        Error("Failed to create GCS client:", err)
        return fmt.Errorf("failed to create GCS client: %v", err)
    }
    defer client.Close()

    bucket := client.Bucket(gcsBucket)
    fileName := fmt.Sprintf("%s/kafka_stream_%d.json", appname, time.Now().UnixNano())
    object := bucket.Object(fileName)
    writer := object.NewWriter(ctx)

    var msgData map[string]interface{}
    if err := json.Unmarshal(message.Value, &msgData); err != nil {
        Error("Failed to unmarshal message value:", err)
        return fmt.Errorf("failed to unmarshal message value: %v", err)
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
        return fmt.Errorf("failed to marshal message: %v", err)
    }

    if _, err := writer.Write(jsonMessage); err != nil {
        Error("Failed to write message to GCS:", err)
        return fmt.Errorf("failed to write message to GCS: %v", err)
    }
    _, _ = writer.Write([]byte("\n"))

    if err := writer.Close(); err != nil {
        Error("Failed to close GCS writer:", err)
        return fmt.Errorf("failed to close GCS writer: %v", err)
    }
    Success("Message streamed to a GCS file successfully")
    return nil
}

func ProcessBatchToBigQuery(projectID, gcsBucket, appname, datasetID, tableID string) error {
    ctx := context.Background()
    client, err := storage.NewClient(ctx)
    if err != nil {
        Error("Failed to create GCS client:", err)
        return fmt.Errorf("failed to create GCS client: %v", err)
    }
    defer client.Close()

    bqClient, err := bigquery.NewClient(ctx, projectID)
    if err != nil {
        Error("Failed to create BigQuery client:", err)
        return fmt.Errorf("failed to create BigQuery client: %v", err)
    }
    defer bqClient.Close()

    bucket := client.Bucket(gcsBucket)
    query := &storage.Query{Prefix: fmt.Sprintf("%s/kafka_stream_", appname)}
    it := bucket.Objects(ctx, query)
    var gcsURIs []string
    for {
        attrs, err := it.Next()
        if err == iterator.Done {
            break
        }

        if err != nil {
            Error("Failed to list objects:", err)
            return fmt.Errorf("failed to list objects: %v", err)
        }

        gcsURI := fmt.Sprintf("gs://%s/%s", gcsBucket, attrs.Name)
        gcsURIs = append(gcsURIs, gcsURI)
    }

    if len(gcsURIs) == 0 {
        Warn("No objects found in bucket with prefix:", query.Prefix)
        return fmt.Errorf("no objects found in bucket with prefix: %s", query.Prefix)
    }

    gcsRef := bigquery.NewGCSReference(gcsURIs...)
    gcsRef.SourceFormat = bigquery.JSON
    gcsRef.Schema = bigquery.Schema{
        {Name: "inserted_at", Type: bigquery.StringFieldType},
        {Name: "app_name", Type: bigquery.StringFieldType},
        {Name: "payload", Type: bigquery.StringFieldType},
        {Name: "extracted_at", Type: bigquery.StringFieldType},
    }
    loader := bqClient.Dataset(datasetID).Table(tableID).LoaderFrom(gcsRef)
    job, err := loader.Run(ctx)
    if err != nil {
        Error("Failed to create BigQuery job:", err)
        return fmt.Errorf("failed to create BigQuery job: %v", err)
    }

    status, err := job.Wait(ctx)
    if err != nil {
        Error("Failed to wait for BigQuery job:", err)
        return fmt.Errorf("failed to wait for BigQuery job: %v", err)
    }
    if err := status.Err(); err != nil {
        Error("BigQuery job completed with error:", err)
        return fmt.Errorf("BigQuery job completed with error: %v", err)
    }
    Success("BigQuery job completed successfully for files with prefix:", query.Prefix)

    for _, gcsURI := range gcsURIs {
        objectName := gcsURI[len(fmt.Sprintf("gs://%s/", gcsBucket)):]
        object := bucket.Object(objectName)
        if err := object.Delete(ctx); err != nil {
            Error("Failed to delete GCS object:", err)
            return fmt.Errorf("failed to delete GCS object: %v", err)
        }
    }

    return nil
}