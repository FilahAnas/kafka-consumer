package main

import (
    "context"
    "runtime"
    "time"
    "github.com/segmentio/kafka-go"
    "go-kafka-consumer/app/utils"
    "github.com/DTSL/golang-libraries/kafkautils"
    "go-kafka-consumer/app/source"
    "go-kafka-consumer/app/transform"
)

func Source(config_url string) (kafka.ReaderConfig, func(context.Context, error)) {
    if err := source.LoadConfig(config_url); err != nil {
        utils.Error("Configuration error: ", map[string]interface{}{
            "error": err,
        })
        return kafka.ReaderConfig{}, nil
    }
    readerConfig := kafka.ReaderConfig{
        Brokers: []string{source.BrokerAddress},
        Topic:   source.Topic,
        GroupID: source.GroupID,
    }
    errorHandler := func(ctx context.Context, er error) {
        utils.Error("Error: ", map[string]interface{}{
            "error": er,
        })
    }
    return readerConfig, errorHandler
}

func Transform(ctx context.Context, readerConfig kafka.ReaderConfig, errorHandler func(context.Context, error)) func(ctx context.Context, messages []kafka.Message) error {
    return transform.ProcessMessage
}

func Destination(ctx context.Context, readerConfig kafka.ReaderConfig, processMessage func(ctx context.Context, messages []kafka.Message) error, errorHandler func(context.Context, error)) {
    kafkautils.RunBatchConsumers(
        ctx,
        readerConfig,
        processMessage,
        runtime.GOMAXPROCS(0),
        source.BatchSize,
        5*time.Second,
        errorHandler)
}

func main() {
    utils.Startup()

    ctx := context.Background()

    readerConfig, errorHandler := Source("config.json")

    processMessage := Transform(ctx, readerConfig, errorHandler)

    Destination(ctx, readerConfig, processMessage, errorHandler)
}