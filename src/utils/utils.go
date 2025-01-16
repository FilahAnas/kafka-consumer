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
	"log"
)


func StreamMessageToGCS(gcsBucket string, appname string, message kafka.Message) error {
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
		return fmt.Errorf("failed to marshal message: %v", err)
	}

	if _, err := writer.Write(jsonMessage); err != nil {
		return fmt.Errorf("failed to write message to GCS: %v", err)
	}
	_, _ = writer.Write([]byte("\n"))

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close GCS writer: %v", err)
	}
	log.Println("Message streamed to a GCS file successfully")
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
        gcsRef.Schema = bigquery.Schema{
            {Name: "inserted_at", Type: bigquery.StringFieldType},
            {Name: "app_name", Type: bigquery.StringFieldType},
            {Name: "payload", Type: bigquery.StringFieldType},
            {Name: "extracted_at", Type: bigquery.StringFieldType},
        }
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
		log.Printf("BigQuery job completed successfully for file: %s", attrs.Name)

        // Delete the file after processing
        // object := bucket.Object(attrs.Name)
        // if err := object.Delete(ctx); err != nil {
        //     return fmt.Errorf("failed to delete GCS object: %v", err)
        // }
    }

    return nil
}