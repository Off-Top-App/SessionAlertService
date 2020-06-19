from kafka import KafkaProducer
from json import dumps
from time import sleep
from pymongo import MongoClient
from json import loads
import random
from flask import Flask, flash, render_template, request, jsonify
from flask_cors import CORS, cross_origin
from flask_pymongo import PyMongo
import sys
from werkzeug.utils import cached_property


app= Flask(__name__)
app.config['MONGO_DBNAME'] = 'offtop-kafka-mongodb'
app.config['MONGO_URI'] = 'mongodb+srv://off-top:<offtoppassword>@off-top-kafka-mogsf.mongodb.net/off-top'
app.config['MONGO_USER'] = 'off-top'
app.config['MONGO_PASSWORD'] = 'offtoppassword'
mongo = PyMongo()
mongo.init_app(app)

def producer():
    producer= KafkaProducer(bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: dumps(x).encode('utf-8'))
    while True:
        if(not oneZero()):
            #message= {'user_id': random.randint(1,111), 'focus_score': oneZero(), 'time': str(datetime.utcnow())}
            message= Event(random.randint(1,111), oneZero(), str(datetime.utcnow()))
            data= dumps(message)
            producer.send('OutgoingFocusAlert', value= data)
            producer.flush()
            print("Data:", data)
            sleep(3)

def oneZero():
    rand= random.randint(0,1)
    if (rand == 0):
        return False
    else:
        return True

if __name__ == '__main__':
    app.run(debug=True)
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:' +
               '%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO)
    try:
        producer()
        print("We made it.")
    except KeyboardInterrupt:
        print("We made it here.")
        sys.exit()
