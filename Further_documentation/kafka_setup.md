# Kafka Installation and Stream Setup

This guide will walk you through the process of setting up a Kafka container and creating a Kafka stream.

## Prerequisites

Before you begin, make sure you have Docker installed on your machine. If you don't have Docker installed, you can download it from [here](https://www.docker.com/products/docker-desktop).

## Running the Kafka Container

1. Open a terminal and navigate to the directory containing the `docker-compose.yml` file.

2. Run the following command to start the Kafka container:

   ```shell
   docker compose up
   ```

3. Once the container is up and running, you can access it using the following command:

   ```shell
   docker exec -it kafka bash
   ```

4. Inside the container, create a topic that our Kafka producer and Kafka consumer will connect to for sending and receiving data:

   ```shell
   kafka-topics --bootstrap-server kafka --create --topic kafka-tech-layoffs
   ```

## Kafka Stream

The Kafka producer is already being executed by Airflow. To view the entire process in real time and verify that the data is being sent to the database and the real-time report, you need to run the consumer.

1. Open a new terminal.

2. Run the `tech_layoffs_consumer.py` file using the following command:
   ```shell
   python3 Scripts/tech_layoffs_consumer.py
   ```

This will start the consumer, and you should be able to see the data being processed in real time.