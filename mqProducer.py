from retry import retry
from pika.exceptions import AMQPConnectionError
from selenium.common.exceptions import TimeoutException
import pika
import datetime
import json
import requests
import logging

#连接mq
def conn(self):
    connection = pika.BlockingConnection(self.parameters)  # 创建连接
    connection.process_data_events()
    channel = connection.channel()  # 建立管道
    return channel
def closeChannel():
    connection.close()

def SaveAtMQ(self, jsonData, ch, method):  # 写入MQ   若连接出现问题 会一直重复这里的操作
    channel = self.conn()
    channel.queue_declare(queue='AmazonReviewCrawler', durable=True)  # 是否队列持久化
    channel.basic_publish(exchange='',  # 交换机
                            routing_key='AmazonReviewCrawler',  # 路由键，写明将消息发往哪个队列
                            body=f'{jsonData}',
                            properties=pika.BasicProperties(
                                delivery_mode=2, )  # delivery_mode=2 消息持久化
                            )  # 生产者要发送的消息
    ch.basic_ack(
        delivery_tag=method.delivery_tag)  # 1.消息生产者端发送消息时挂掉了,消费者接消息时挂掉了, rabbitMQ会让改消息重新回到消息队列中       2.手动向MQ确认消费
    channel.closeChannel()  # 当生产者发送完消息后，可选择关闭连接






#数据格式示例


