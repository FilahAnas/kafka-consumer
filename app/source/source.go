package source

import (
    "encoding/json"
    "log"
    "os"
    "fmt"
)

var (
    Topic         string
    BrokerAddress string
    GroupID       string
    BatchSize     int
    Timeout       int
    Destination   DestinationConfig
)

type DestinationConfig struct {
    Pubsub struct {
        Topic string `json:"topic"`
    } `json:"pubsub"`
    Bigquery struct {
        GcsBucket string `json:"gcsBucket"`
        BqDataset string `json:"bqDataset"`
        BqTable   string `json:"bqTable"`
        ProjectID string `json:"projectId"`
    } `json:"bigquery"`
    AppName string `json:"appName"`
}

type Config struct {
    Source struct {
        Topic         string `json:"topic"`
        BrokerAddress string `json:"brokerAddress"`
        GroupID       string `json:"groupId"`
    } `json:"source"`
    Transform struct {
        BatchSize int `json:"batchSize"`
        Timeout   int `json:"timeout"`
    } `json:"transform"`
    Destination DestinationConfig `json:"destination"`
}

func LoadConfig(filePath string) error {
    file, err := os.Open(filePath)
    if (err != nil) {
        return fmt.Errorf("could not open config file: %v", err)
    }
    defer file.Close()

    var config Config
    decoder := json.NewDecoder(file)
    if err := decoder.Decode(&config); err != nil {
        return fmt.Errorf("could not decode config JSON: %v", err)
    }

    if config.Source.Topic == "" || config.Source.BrokerAddress == "" || config.Source.GroupID == "" {
        return fmt.Errorf("source configuration fields cannot be empty")
    }

    Topic = config.Source.Topic
    BrokerAddress = config.Source.BrokerAddress
    GroupID = config.Source.GroupID
    BatchSize = config.Transform.BatchSize
    Destination = config.Destination
    Timeout = config.Transform.Timeout

    if BatchSize <= 0 {
        return fmt.Errorf("batch size must be a positive integer")
    }
    if Timeout <= 0 {
        return fmt.Errorf("timeout must be a positive integer")
    }

    log.Printf("Configuration loaded successfully: %+v", config)
    return nil
}