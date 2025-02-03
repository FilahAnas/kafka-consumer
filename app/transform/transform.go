package transform

import (
    "context"
    "sync"
    "github.com/segmentio/kafka-go"
    "go-kafka-consumer/app/utils"
    "go-kafka-consumer/app/source"
)

func ProcessMessage(ctx context.Context, messages []kafka.Message) error {
    var wg sync.WaitGroup
    successCount := 0
    errorCount := 0

    for _, message := range messages {
        wg.Add(1)
        go func(msg kafka.Message) {
            defer wg.Done()
            if len(msg.Value) > 0 {
                if err := utils.StreamMessageToGCS(source.Destination.Bigquery.GcsBucket, source.Destination.AppName, msg); err != nil {
                    utils.Error("Failed to stream message to GCS", map[string]interface{}{
                        "error": err,
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
        "successCount":  successCount,
        "totalMessages": len(messages),
    })

    return nil
}