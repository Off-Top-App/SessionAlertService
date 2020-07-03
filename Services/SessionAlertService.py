from pymongo import MongoClient
from flask_pymongo import PyMongo
import json
mongo = PyMongo()


def postAlertData(data):
    session_alerts= mongo.db.sessionAlerts
    value= json.loads(data)
    inserted= session_alerts.insert({'user_id': value['user_id'], 'focus_score': value['focus_score'], 'time': value['time']})
