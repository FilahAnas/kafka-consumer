# Go Kafka Consumer

## Running the Application

To run the application, use the following command:

```sh
make run
```

## Testing Code Locally

### 1. Run Kafka

1. **Download Kafka**

2. **Build Kafka JAR**
    ```sh
    ./gradlew jar -PscalaVersion=2.13.14
    ```

3. **Start Zookeeper**
    ```sh
    bin/zookeeper-server-start.sh config/zookeeper.properties
    ```

4. **Start Kafka Server** (in another terminal)
    ```sh
    bin/kafka-server-start.sh config/server.properties
    ```

5. **Add New Topic**
    ```sh

    bin/kafka-topics.sh --create --topic brevo-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
    bin/kafka-configs.sh --alter --entity-type topics --entity-name brevo-data --add-config retention.ms=1000 --bootstrap-server localhost:9092

    ```

6. **List All Topics**
    ```sh
    bin/kafka-topics.sh --list --bootstrap-server localhost:9092
    ```

7. **Produce Message**
    ```sh
    bin/kafka-console-producer.sh --topic brevo-data --bootstrap-server localhost:9092
    ```

8. **Consume Message**
    ```sh
    bin/kafka-console-consumer.sh --topic brevo-data --from-beginning --bootstrap-server localhost:9092
    ```
# kafka-consumer
