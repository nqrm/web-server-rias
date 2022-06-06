from flask import Flask
from flask import request
from pymongo import MongoClient
from redis import Redis
from bson.json_util import dumps
import pika
import json

app = Flask(__name__)

redis = Redis(host='redis', port=6379,charset="utf-8", decode_responses=True)

client = MongoClient('mongodb://mongo:27017')
db = client['flaskdb']
collection = db['Chat']

@app.route("/sendMessage",methods = ['POST'])
def sendMessage():
    req = request.json
    username = req['username']
    message = req['message']
    connection = pika.BlockingConnection(pika.URLParameters('amqp://admin:admin@rabbit:5672'))
    channel = connection.channel()
    channel.queue_declare(queue='tasks',durable =True)
    task = {'username': username,'message': message}
    channel.basic_publish(exchange='',
                          routing_key='tasks',
                          body=json.dumps(task),
                          properties=pika.BasicProperties(
                              delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                          ))
    connection.close()
    return "Success sent"

@app.route("/getChat",methods = ['GET'])
def getChat():
    chat = ""
    try:
        parsedChat = json.loads(redis.get('chat'))
        for obj in parsedChat:
            chat += "{}:{}\n".format(obj['username'],obj['message'])
        return chat
    except:
        app.logger.info("Cache is empty")

    chatdb = collection.find()
    serializedObj = dumps(chatdb)
    result = redis.set('chat', serializedObj, 600)
    parsedChat = json.loads(redis.get('chat'))
    for obj in parsedChat:
        chat += "{}:{}\n".format(obj['username'], obj['message'])

    return chat
