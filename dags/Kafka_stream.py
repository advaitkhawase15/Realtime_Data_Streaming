from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
import time
import requests
import json
import logging

default_args = {
    'owner': 'Advait',
    'start_date': datetime(2025,5,15,00,00)
}

def get_data():
    #https://random-data-api.com/api/users/random_user
    response = requests.get("https://dummyjson.com/users/2")
    res = response.json()
    return res

def format_data(res):
    data = {
        "first_name": res["firstName"],
        "last_name": res["lastName"],
        "gender": res["gender"],
        "address": ",".join([
            res["address"]["address"],
            res["address"]["city"],
            res["address"]["state"],
            res["address"]["country"]
        ]),
        "postcode": res["address"]["postalCode"],
        "email": res["email"],
        "dob": res["birthDate"],
        "picture": res["image"],
        "phone": res["phone"]
    }
    return data

def stream_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'],max_block_ms=5000)
    curr_time = time.time()
    while True:
        if time.time() > curr_time + 60:
            break
        try:
            res = get_data()
            res = format_data(res)
            producer.send('user_created',json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured:{e}')
            continue

with DAG('task_id', 
        default_args = default_args,
        schedule_interval = None,
        catchup=False)as dag:

    streaming_task = PythonOperator(
        task_id = 'stream_data_from_api',
        python_callable = stream_data
    )

# stream_data()
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
import time
import requests
import json
import logging

default_args = {
    'owner': 'Advait',
    'start_date': datetime(2025,5,15,00,00)
}

def get_data():
    #https://random-data-api.com/api/users/random_user
    response = requests.get("https://dummyjson.com/users/2")
    res = response.json()
    return res

def format_data(res):
    data = {
        "first_name": res["firstName"],
        "last_name": res["lastName"],
        "gender": res["gender"],
        "address": ",".join([
            res["address"]["address"],
            res["address"]["city"],
            res["address"]["state"],
            res["address"]["country"]
        ]),
        "postcode": res["address"]["postalCode"],
        "email": res["email"],
        "dob": res["birthDate"],
        "picture": res["image"],
        "phone": res["phone"]
    }
    return data

def stream_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'],max_block_ms=5000)
    curr_time = time.time()
    while True:
        if time.time() > curr_time + 60:
            break
        try:
            res = get_data()
            res = format_data(res)
            producer.send('user_created',json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured:{e}')
            continue

with DAG('task_id', 
        default_args = default_args,
        schedule_interval = None,
        catchup=False)as dag:

    streaming_task = PythonOperator(
        task_id = 'stream_data_from_api',
        python_callable = stream_data
    )

# stream_data()
