from kafka import KafkaProducer
import json
from pymongo import MongoClient
import random
import logging
from flask_pymongo import PyMongo
import sys
from werkzeug.utils import cached_property
from Event import Event
from _datetime import datetime
from time import sleep


app= Flask(__name__)
app.config['MONGO_DBNAME'] = 'offtop-kafka-mongodb'
app.config['MONGO_URI'] = 'mongodb+srv://off-top:offtoppassword@off-top-kafka-mogsf.mongodb.net/off-top'
app.config['MONGO_USER'] = 'off-top'
app.config['MONGO_PASSWORD'] = 'offtoppassword'
mongo = PyMongo()
mongo.init_app(app)

def producer():
    producer= KafkaProducer(bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    while True:
        if(not getRandomFocusScore()):
            focusAlert= Event(random.randint(1,111), oneZero(), str(datetime.utcnow())).__dict__
            data= json.dumps(focusAlert)
            producer.send('OutgoingFocusAlert', value= data)
            producer.flush()
            print("Data:", data)
            sleep(20)

def getRandomFocusScore():
    rand= random.randint(0,1)
    if (rand == 0):
        return False
    else:
        return True

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:' +
               '%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO)
    try:
        producer()
    except KeyboardInterrupt:
        sys.exit()
