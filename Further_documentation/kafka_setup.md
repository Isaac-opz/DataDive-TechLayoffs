## ---Kafka Installation---

##  Running the Kafka container 

###  Make sure to have Docker installed and run this command with the docker-compose.yml file open in a terminal
```docker compose up```

### Access the Kafka container
``` docker exec -it kafka bash ```

### Once inside the container, we will create a topic to which our Kafka producer and Kafka consumer will connect to send and receive data
``` kafka-topics --bootstrap-server kafka --create --topic kafka-tech-layoffs ```

## ---Kafka Stream---

### Make sure to open a terminal to run the consumer. The producer is already being executed by Airflow. Run the tech_layoffs_consumer.py file in the terminal to view the entire process in real time and verify that the data is being sent to the database and the real-time report.

``` python3 Scripts/tech_layoffs_consumer.py ```