package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
	"github.com/DTSL/golang-libraries/kafkautils"
	"github.com/segmentio/kafka-go"
	"go-kafka-consumer/src/utils"     // Import du package utils
)

// Déclaration des variables globales
var (
	topic         string
	brokerAddress string
	groupID       string
	gcsBucket     string
	bqDataset     string
	bqTable       string
	batchSize     int
	projectID     string
	appname       string
)

// Fonction pour charger les variables d'environnement
func loadEnvVars() error {
	var err error
	topic = os.Getenv("TOPIC")
	brokerAddress = os.Getenv("BROKER_ADDRESS")
	groupID = os.Getenv("GROUP_ID")
	gcsBucket = os.Getenv("GCS_BUCKET")
	bqDataset = os.Getenv("BQ_DATASET")
	bqTable = os.Getenv("BQ_TABLE")
	projectID = os.Getenv("PROJECT_ID")
	appname = os.Getenv("APP_NAME")

	if topic == "" || brokerAddress == "" || groupID == "" || gcsBucket == "" || bqDataset == "" || bqTable == "" || projectID == "" {
		return fmt.Errorf("missing one or more environment variables")
	}

	batchSize, err = strconv.Atoi(os.Getenv("BATCH_SIZE"))
	if err != nil || batchSize <= 0 {
		return fmt.Errorf("BATCH_SIZE must be a valid integer greater than 0: %v", err)
	}

	return nil
}

// Fonction de traitement des lots de messages Kafka
func processBatchMessages(ctx context.Context, messages []kafka.Message) error {
	log.Printf("Traitement d'un lot de %d messages...", len(messages))
	for _, message := range messages {
		// Exemple de traitement : envoyer à Google Cloud Storage (GCS)
		if len(message.Value) > 0 {
			if err := utils.StreamMessageToGCS(gcsBucket, appname, message); err != nil {
				log.Printf("Erreur lors de l'envoi du message à GCS : %v", err)
				return err
			}
		} else {
			log.Println("Message vide reçu, ignoré.")
		}
	}

	// Traitement des messages accumulés dans BigQuery
	if err := utils.ProcessBatchToBigQuery(projectID, gcsBucket, appname, bqDataset, bqTable); err != nil {
		log.Printf("Erreur lors du traitement des messages vers BigQuery : %v", err)
		return err
	}

	return nil
}

func main() {
	// Initialisation des dépendances de l'application
	utils.Startup()

	// Chargement des variables d'environnement
	if err := loadEnvVars(); err != nil {
		log.Fatalf("Erreur de configuration : %v", err)
	}

	// Configuration du lecteur Kafka
	readerConfig := kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		GroupID: groupID,
	}

	// Définir une fonction pour gérer les erreurs pendant la consommation
	handleError := func(ctx context.Context, err error) {
		log.Printf("Erreur lors de la consommation des messages : %v", err)
	}

	// Exécuter les consommateurs Kafka avec kafkautils
	ctx := context.Background()
	kafkautils.RunBatchConsumers(
		ctx,
		readerConfig,         
		processBatchMessages, 
		3,                    
		batchSize,           
		10*time.Minute,       
		handleError,         
	)
}
