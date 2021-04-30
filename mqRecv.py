from selenium.webdriver.chrome.options import Options  # 无界面
# from selenium.webdriver.firefox.options import Options # 火狐
from selenium.common.exceptions import TimeoutException
from concurrent.futures import ThreadPoolExecutor
from pika.exceptions import AMQPConnectionError
from fateadm_api import TestFunc
from selenium import webdriver
from retry import retry
import requests
import datetime
import logging
import hashlib
import json
import time
import pika
import re
import os




if __name__ == '__main__':
    username = 'jc_crawler'  # 指定远程rabbitmq的用户名密码
    pwd = 'Jinchang001'
    user_pwd = pika.PlainCredentials(username, pwd)
    parameters = pika.ConnectionParameters('192.168.2.214',credentials=user_pwd)
    connection = pika.BlockingConnection(parameters)  # 创建连接
    connection.process_data_events()

    channel = connection.channel()  # 建立管道
    channel.queue_declare(queue='IndexReview', durable=True)  # 是否队列持久化


    def callback(ch, method, properties, body):
        print(f" [x] Received {body}{type(body)}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    channel.basic_consume('IndexReview',callback)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

    connection.close()  

