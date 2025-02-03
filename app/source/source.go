package source

import (
	"encoding/json"
	"fmt"
	"os"
)

var (
	Topic         string
	BrokerAddress string
	GroupID       string
	GcsBucket     string
	BqDataset     string
	BqTable       string
	BatchSize     int
	ProjectID     string
	Appname       string
	Timeout       int
)

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
	Destination struct {
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
	} `json:"destination"`
}

func LoadConfig(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("could not open config file: %v", err)
	}
	defer file.Close()

	var config Config
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		return fmt.Errorf("could not decode config JSON: %v", err)
	}

	Topic = config.Source.Topic
	BrokerAddress = config.Source.BrokerAddress
	GroupID = config.Source.GroupID
	GcsBucket = config.Destination.Bigquery.GcsBucket
	BqDataset = config.Destination.Bigquery.BqDataset
	BqTable = config.Destination.Bigquery.BqTable
	BatchSize = config.Transform.BatchSize
	ProjectID = config.Destination.Bigquery.ProjectID
	Appname = config.Destination.AppName
	Timeout = config.Transform.Timeout
	if BatchSize <= 0 {
		return fmt.Errorf("BATCH_SIZE must be a valid integer greater than 0")
	}
	if Timeout <= 0 {
		return fmt.Errorf("TIMEOUT must be a valid integer greater than 0")
	}

	return nil
}
