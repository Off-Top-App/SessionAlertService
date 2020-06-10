from kafka import KafkaProducer
from json import dumps
from time import sleep
from flask import Flask, flash, render_template, request, jsonify
from flask_cors import CORS, cross_origin
from flask_restplus import Api
from extensions import mysql, mongo

app= Flask(__name__)
app.config['MONGO_DBNAME'] = 'offtop-kafka-mongodb'
app.config['MONGO_URI'] = ''
app.config['MONGO_USER'] = ''
app.config['MONGO_PASSWORD'] = ''
mongo.init_app(app)

def producer():
    producer= KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))


if __name__ == '__main__':
    producer()
    app.run(debug=True)
