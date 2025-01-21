package utils

import (
    "fmt"
    "log"
    "os"
)

const (
    Reset  = "\033[0m"
    Red    = "\033[31m"
    Green  = "\033[32m"
    Yellow = "\033[33m"
    Blue   = "\033[34m"
)

var (
    InfoLogger  = log.New(os.Stdout, fmt.Sprintf("%s[INFO] %s", Blue, Reset), log.Ldate|log.Ltime|log.Lshortfile)
    WarnLogger  = log.New(os.Stdout, fmt.Sprintf("%s[WARN] %s", Yellow, Reset), log.Ldate|log.Ltime|log.Lshortfile)
    ErrorLogger = log.New(os.Stderr, fmt.Sprintf("%s[ERROR] %s", Red, Reset), log.Ldate|log.Ltime|log.Lshortfile)
    SuccessLogger = log.New(os.Stdout, fmt.Sprintf("%s[SUCCESS] %s", Green, Reset), log.Ldate|log.Ltime|log.Lshortfile)
)

func Info(v ...interface{}) {
    InfoLogger.Println(v...)
}

func Warn(v ...interface{}) {
    WarnLogger.Println(v...)
}

func Error(v ...interface{}) {
    ErrorLogger.Println(v...)
}

func Success(v ...interface{}) {
    SuccessLogger.Println(v...)
}

func Startup() {

banner := `
  ____ _____           ____ ___  _   _ ____  _   _ __  __ _____ ____  
 |  _ \_   _|         / ___/ _ \| \ | / ___|| | | |  \/  | ____|  _ \ 
 | | | || |   _____  | |  | | | |  \| \___ \| | | | |\/| |  _| | |_) |
 | |_| || |  |_____| | |__| |_| | |\  |___) | |_| | |  | | |___|  _ < 
 |____/ |_|           \____\___/|_| \_|____/ \___/|_|  |_|_____|_| \_\
                                                                                                                                        
`
fmt.Println(Green + banner + Reset)
InfoLogger.Println("Brevo application starting...")
}