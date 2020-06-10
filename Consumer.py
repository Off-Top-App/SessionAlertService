from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
from flask_pymongo import PyMongo
import json
from datetime import datetime
import logging
import sys

app= Flask(__name__)
app.config['MONGO_DBNAME'] = 'offtop-kafka-mongodb'
app.config['MONGO_URI'] = ''
app.config['MONGO_USER'] = ''
app.config['MONGO_PASSWORD'] = ''
mongo.init_app(app)

def conumer():
    consumer= KafkaConsumer(
        'SessionAlerts',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit= True,
        group_id= 'my-group',
        value_deserializer= lambda x: loads(x.decode('utf-8')))

@app.route('/consume', methods=['POST'])
def postData():
    collection= mongo.db.sessionAlerts
    

if __name__ == '__main__':
    consumer()
    app.run(debug= True)
