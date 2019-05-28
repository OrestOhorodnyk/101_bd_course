This script generates messages and publish them to spesific kafka topic.
Message format - JSON, message structure predefined.
Message data generated randomly. It is possible to run this app in multithreading end specify the number of messages per thread


# Requirements:
## python 3.6 +
## kafka 1.3.5

# Configuration
# In app/config the one can specify kafka host and port as well es topic name to publish messages 

# Parameters
## --t nubmer of threads, default value 5
## --n nubmer of messages per thread, default value 500
# RUN:
`python3.6 app/main.py --t=<number of threads> --n=<number of messages per thread>`



