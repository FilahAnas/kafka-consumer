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

func ProcessBatchToBigQuery(projectID, gcsBucket, datasetID, tableID string) error {
    ctx := context.Background()
    client, err := storage.NewClient(ctx)
    if err != nil {
        return fmt.Errorf("failed to create GCS client: %v", err)
    }
    defer client.Close()

    bqClient, err := bigquery.NewClient(ctx, projectID)
    if err != nil {
        return fmt.Errorf("failed to create BigQuery client: %v", err)
    }
    defer bqClient.Close()

    bucket := client.Bucket(gcsBucket)
    query := &storage.Query{Prefix: "kafka_stream_"}
    it := bucket.Objects(ctx, query)

    for {
        attrs, err := it.Next()
        if err == iterator.Done {
            break
        }
        if err != nil {
            return fmt.Errorf("failed to list objects: %v", err)
        }

        gcsURI := fmt.Sprintf("gs://%s/%s", gcsBucket, attrs.Name)
        gcsRef := bigquery.NewGCSReference(gcsURI)
        gcsRef.SourceFormat = bigquery.JSON

        loader := bqClient.Dataset(datasetID).Table(tableID).LoaderFrom(gcsRef)
        job, err := loader.Run(ctx)
        if err != nil {
            return fmt.Errorf("failed to create BigQuery job: %v", err)
        }

        status, err := job.Wait(ctx)
        if err != nil {
            return fmt.Errorf("failed to wait for BigQuery job: %v", err)
        }
        if err := status.Err(); err != nil {
            return fmt.Errorf("BigQuery job completed with error: %v", err)
        }

        // Delete the file after processing
        object := bucket.Object(attrs.Name)
        if err := object.Delete(ctx); err != nil {
            return fmt.Errorf("failed to delete GCS object: %v", err)
        }
    }

    return nil
}