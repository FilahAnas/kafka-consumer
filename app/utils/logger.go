package utils

import (
    "github.com/sirupsen/logrus"
    "os"
    "fmt"
)

var log = logrus.New()

func init() {
    log.Out = os.Stdout
    log.SetFormatter(&logrus.JSONFormatter{})
}

func Info(message string, fields map[string]interface{}) {
    log.WithFields(fields).Info(message)
}

func Warn(message string, fields map[string]interface{}) {
    log.WithFields(fields).Warn(message)
}

func Error(message string, fields map[string]interface{}) {
    log.WithFields(fields).Error(message)
}

func Success(message string, fields map[string]interface{}) {
    log.WithFields(fields).Info(message) // Vous pouvez créer un niveau personnalisé si nécessaire
}

func Startup() {
    banner := `
  ____ _____           ____ ___  _   _ ____  _   _ __  __ _____ ____  
 |  _ \_   _|         / ___/ _ \| \ | / ___|| | | |  \/  | ____|  _ \ 
 | | | || |   _____  | |  | | | |  \| \___ \| | | | |\/| |  _| | |_) |
 | |_| || |  |_____| | |__| |_| | |\  |___) | |_| | |  | | |___|  _ < 
 |____/ |_|           \____\___/|_| \_|____/ \___/|_|  |_|_____|_| \_\
                                                                                                                                        
`
    Info("Application starting...", map[string]interface{}{"app": "Data team consumer"})
    fmt.Println(banner)
}