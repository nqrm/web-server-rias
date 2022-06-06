import pika, sys, os
from pymongo import MongoClient
from redis import Redis
import time
from bson.json_util import dumps
import json

time.sleep(10)

client = MongoClient('mongodb://mongo:27017')
db = client['flaskdb']
collection = db['Chat']

redis = Redis(host='redis', port=6379,charset="utf-8", decode_responses=True)

connection = pika.BlockingConnection(pika.URLParameters('amqp://admin:admin@rabbit:5672'))
channel = connection.channel()
channel.queue_declare(queue='tasks', durable = True)

def SendMessage(ch, method, properties, body):
    ch.basic_ack(delivery_tag=method.delivery_tag)
    collection.insert_one(json.loads(body)).inserted_id
    chat = collection.find()
    serializedObj = dumps(chat)
    result = redis.set('chat',serializedObj,600)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='tasks', on_message_callback=SendMessage)
channel.start_consuming()


#bebra