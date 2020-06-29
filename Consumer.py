from kafka import KafkaConsumer
from pymongo import MongoClient
from flask_pymongo import PyMongo
import json
from flask_pymongo import PyMongo
from Services.SessionAlertServices import postAlertData

app= Flask(__name__)
app.config['MONGO_DBNAME'] = 'offtop-kafka-mongodb'
app.config['MONGO_URI'] = 'mongodb+srv://off-top:offtoppassword@off-top-kafka-mogsf.mongodb.net/off-top'
app.config['MONGO_USER'] = 'off-top'
app.config['MONGO_PASSWORD'] = 'offtoppassword'
mongo = PyMongo()
mongo.init_app(app)

def startConsumer():
    consumer= KafkaConsumer(
        'NewFocusAlert',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit= True,
        group_id= 'my-group',
    )
    KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('utf-8')))

    for message in consumer:
        consumed_value= json.loads(message.value.decode('utf-8'))
        postAlertData(consumed_value)
        print("Subscribing to Session Alerts:\nMessage:", consumed_value)

def main():
    startConsumer()

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:' +
               '%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    main()
