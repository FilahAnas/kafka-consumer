# Include .env file if it exists
ifneq (,$(wildcard .env))
    include .env
    export
endif

# Define the run target
run:
	go mod download
	go run src/main.go
