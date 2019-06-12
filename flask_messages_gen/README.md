This application generates messages and publish them to specific kafka topic.
Message format - JSON, message structure predefined.
Message data generated randomly. It is possible to run this app in multithreading end specify the number of messages per thread.

Application the flowing docker conteiner:
* `zookeeper`
* `kafka`
* `app_produser`


# Requirements:
* docker and docker-compose

# Configuration
* In app/produser/config the one can specify topic name to publish messages 


# FIRST RUN:
* `cd flask_messages_gen`
* `docker-compose up -d --build`
It mey take some time to download and run required images

# STOP
* `docker-compose stop`

# START
* `docker-compose start`

# STOP and REMOVE 
* `docker-compose down`

# to generate messages 
* open browser
* navigate to `http://0.0.0.0:5000/`
* enter number of messages and number of threads 
* click the 'Generate' button

# to verify if messages are in kafka
* open terminal
* run `docker exec -it kafka bin/bash`
* get all topics `/opt/kafka_2.12-2.2.1/bin/kafka-topics.sh --list --zookeeper zookeeper:2181`
* see messages in the topic `/opt/kafka_2.12-2.2.1/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --from-beginning --topic <topic name>`



