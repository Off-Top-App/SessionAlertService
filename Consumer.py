from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
from flask_pymongo import PyMongo
import json
from datetime import datetime
import logging
import sys
from json import dumps
from time import sleep
from pymongo import MongoClient
import random
from flask import Flask, flash, render_template, request, jsonify
from flask_cors import CORS, cross_origin
from flask_pymongo import PyMongo


app= Flask(__name__)
app.config['MONGO_DBNAME'] = 'offtop-kafka-mongodb'
app.config['MONGO_URI'] = 'mongodb+srv://off-top:offtoppassword@off-top-kafka-mogsf.mongodb.net/off-top'
app.config['MONGO_USER'] = 'off-top'
app.config['MONGO_PASSWORD'] = 'offtoppassword'
mongo = PyMongo()
mongo.init_app(app)

def startConsumer():
    consumer= KafkaConsumer(
        'OutgoingFocusAlert',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit= True,
        group_id= 'my-group',
    )
    KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    
    for message in consumer:
        consumed_value= loads(message.value.decode('utf-8'))
        postData(consumed_value)
        print("Subscribing to Session Alerts:\nMessage:", consumed_value)

#@app.route('/consume', methods=['POST'])
def postData(data):
    session_alerts= mongo.db.sessionAlerts
    #print("this is value", value[10:14])
    value= json.loads(data)
    inserted= session_alerts.insert({'user_id': value['user_id'], 'focus_score': value['focus_score'], 'time': value['time']})
    #inserted= session_alerts.insert_one(value)

def main():
    startConsumer()

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:' +
               '%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    main()
