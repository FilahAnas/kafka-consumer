
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
    Success("[Batch] - BigQuery job completed successfully for files with prefix:", query.Prefix)

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

