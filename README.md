# Wikimedia Kafka OpenSearch Pipeline

This project demonstrates a simple data engineering pipeline that streams live edits from the [Wikimedia Recent Change](https://stream.wikimedia.org) feed, publishes the events to Apache Kafka, and indexes them into an OpenSearch cluster for searching and visualization.

## Architecture

```
Wikimedia API  -->  Kafka (wiki-topic)  -->  OpenSearch (wikimedia_index)
```

Two small Java applications implement the pipeline:

- **ApiToKafkaProducer** &mdash; connects to the Wikimedia Server-Sent Events (SSE) stream and produces each event to a Kafka topic.
- **KafkaToOpenSearch** &mdash; consumes records from Kafka and stores them in an OpenSearch index.

Docker Compose spins up the required infrastructure (Kafka, Zookeeper, OpenSearch, and OpenSearch Dashboards) for local development.

## Prerequisites

- [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/)
- JDK 17+
- Gradle (wrapper included)

## Getting Started

1. **Start infrastructure**

   ```bash
   export OPENSEARCH_INITIAL_ADMIN_PASSWORD=your-password
   docker-compose up -d
   ```

   OpenSearch will be available at https://localhost:9200 and Dashboards at http://localhost:5601.

2. **Run the API producer**

   ```bash
   ./gradlew run --args='ApiToKafkaProducer'
   ```

   The producer continuously reads Wikimedia events and publishes them to the `wiki-topic` Kafka topic.

3. **Run the Kafka consumer**

   ```bash
   ./gradlew run --args='KafkaToOpenSearch'
   ```

   The consumer polls the `wiki-topic` topic and indexes each event into the `wikimedia_index` index in OpenSearch.

## Project Structure

```
├── docker-compose.yml       # Kafka, Zookeeper, OpenSearch, Dashboards
├── src/main/java
│   ├── ApiToKafkaProducer.java   # SSE -> Kafka
│   └── KafkaToOpenSearch.java    # Kafka -> OpenSearch
```

## Testing

Run the Gradle test suite (contains only basic checks at the moment):

```bash
./gradlew test
```

## Future Work

- Harden OpenSearch security and configuration.
- Package producer and consumer into standalone services or Docker images.
- Add schema validation and structured logging.

---
Feedback and contributions are welcome!
