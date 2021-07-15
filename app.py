import time
import json
import redis
from flask import Flask
from kafka import KafkaProducer
import uuid

app = Flask(__name__)
cache = redis.Redis(host='redis', port=6379)

def get_hit_count():
    retries = 5
    while True:
        try:
            return cache.incr('hits')
        except redis.exceptions.ConnectionError as exc:
            if retries == 0:
                raise exc
            retries -= 1
            time.sleep(0.5)

@app.route('/')
def hello():
    count = get_hit_count()
    return 'Hello World! I have been seen {} times.\n'.format(count)

def generate_content():
    return str(uuid.uuid1())

@app.route('/produce')
def mail_kafka():
    content = generate_content()
    topic="test"
    producer = KafkaProducer(bootstrap_servers='[10.162.0.4:9092]', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    msg = {'content': content} 
    producer.send(topic, msg)
    return f"sent msg = ${msg}"
    
