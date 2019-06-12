from flask import Flask, request
from producer.config import TOPIC_NAME, CONFIGS
from producer.produser import Producer
from producer.consumer import check_if_topic_created, create_topics

app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def gen_messages():
    if request.method == 'POST':
        threads = int(request.values.get('threads'))
        message_count = int(request.values.get('messages'))
        for i in range(1, threads + 1):
            p = Producer(thread_name=f'Thread {i}', number_of_messages=message_count, topic_name=TOPIC_NAME)
            p.start()
    return '''
        <form method="post">
            How many threads?<br>
            <input type=number name=threads>
            <p>What is the number of messages per thread?<br>
            <input type=text name=messages>
            <p><input type=submit value=Generate></p>
        </form>
    '''


is_topic_created = check_if_topic_created(
    bootstrap_servers=[f"{CONFIGS['KAFKA_ADDRESS']}:{CONFIGS['KAFKA_PORT']}"],
    topic_name=TOPIC_NAME
)

if not is_topic_created:
    create_topics(
        bootstrap_servers=[f"{CONFIGS['KAFKA_ADDRESS']}:{CONFIGS['KAFKA_PORT']}"],
        topic_names=[TOPIC_NAME]
    )

app.run(host='0.0.0.0', port=5000)
