#!/usr/bin/env python
import pika
import json
import os
import sys
import configparser
import logging.config
import utils
import vocabulary_etl
import concepts_etl
import mysql.connector
from datetime import datetime
import database

def update_status_task(uuid, result):
    config = configparser.ConfigParser()
    config.read('config.ini')
    database_configuration = config['database_tasks']

    config = {
      'user': database_configuration['db_user'],
      'password': database_configuration['db_password'],
      'host': database_configuration['db_host'],
      'database': database_configuration['db_schema'],
      'raise_on_warnings': True
    }

    print("Connecting to database...")
    cnx = mysql.connector.connect(**config)
    print("The connection to the database was succesfull")
    if result:
        status = "Succeeded"
    else:
        status = "Failed"
    print("uuid: {0} - status: {1}".format(status, uuid))
    database.update_task_status(status, uuid, cnx)
    print("Task status updated")

# Read rabbitmq configuration
config = configparser.ConfigParser()
config.read('config.ini')
database_configuration = config['rabbitmq']

credentials = pika.PlainCredentials(database_configuration['user'],
                                    database_configuration['password'])
connection = pika.BlockingConnection(pika.ConnectionParameters(
                                        database_configuration['host'],
                                        5672,
                                        '/',
                                        credentials))
channel = connection.channel()

channel.queue_declare(queue=database_configuration['queue'])

def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    task = json.loads(body)
    file_id = task['file_id']
    now = datetime.now()
    documentType = task['document_type']
    path_file = "./received_files/{0}/{1}_{2}_{3}.csv".format(documentType,documentType,file_id,
                                                                            now.strftime("%Y%m%d%H%M%S"))
    utils.download_file_from_google_drive(file_id, 
                                          path_file)
    print("executing {0} etl with file: {1}".format(documentType,path_file))
    if(documentType=="vocabulary"):
        resultado = vocabulary_etl.execute(path_file)
    else:
        resultado = concepts_etl.execute(path_file)
    if resultado:
        # update task in success
        update_status_task(task['uuid'], True)
    else:
        # update task in error
        update_status_task(str(task['uuid']), False)

channel.basic_consume(queue=database_configuration['queue'],
                      on_message_callback=callback,
                      auto_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()