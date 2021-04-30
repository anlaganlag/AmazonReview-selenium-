from selenium.webdriver.chrome.options import Options  # 无界面
# from selenium.webdriver.firefox.options import Options # 火狐
from selenium.common.exceptions import TimeoutException
from concurrent.futures import ThreadPoolExecutor
from pika.exceptions import AMQPConnectionError
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
import pymssql


class HandleTask():
    def ReadDBTask(self):
        connect = pymssql.connect('192.168.2.163', 'sa', 'JcEbms123', 'EBMS')  # 服务器名,账户,密码,数据库名
        cursor = connect.cursor()  # 创建执行sql语句对象
        Sql = "SELECT * FROM TbIndexReviewSpiderTask where taskState= 'New'"  # 获取id   Top  每次拿出多少条
        
        cursor.execute(Sql)
        rows = cursor.fetchall()
        connect.close()  # 关闭数据库
        return rows

def GetSiteDomain(site):
    """从站点名获取站点对应的顶级域名"""
    siteDomain = {
            'US':"com",
            'UK':"co.uk"
    }
    return siteDomain[site] if siteDomain.get(site) else site.lower()
def SaveAtMQ( jsonData):  # 写入MQ   若连接出现问题 会一直重复这里的操作
        listItem = json.loads(jsonData)
        connection = pika.BlockingConnection(parameters)  # 创建连接
        connection.process_data_events()  # 在执行长时间任务时，定时调用 process_data_events 方法，就不会丢失连接
        channel = connection.channel()  # 建立管道
        channel.queue_declare(queue='IndexReview', durable=True)  # 是否队列持久化
        for item in listItem:
            channel.basic_publish(exchange='',  # 交换机
                                routing_key='IndexReview',  # 路由键，写明将消息发往哪个队列
                                body=f'{json.dumps(item)}',
                                properties=pika.BasicProperties(
                                    delivery_mode=2, )  # delivery_mode=2 消息持久化
                                )  # 生产者要发送的消息
 # 1.消息生产者端发送消息时挂掉了,消费者接消息时挂掉了, rabbitMQ会让改消息重新回到消息队列中       2.手动向MQ确认消费
        connection.close()  # 当生产者发送完消息后，可选择关闭连接




if __name__ == '__main__':
    user_pwd = pika.PlainCredentials('jc_crawler' , 'Jinchang001')
    parameters = pika.ConnectionParameters('192.168.2.214',credentials=user_pwd)
    Task = HandleTask()
    DbDataRows = Task.ReadDBTask()
    ListTaskUrl = []
    for taskid,Site,Asin,*rest in DbDataRows:
        itemTask = {}
        itemTask['taskid'] = taskid
        itemTask['Site'] = Site
        itemTask['Asin'] = Asin
        itemTask['taskurl'] = f"https://www.amazon.{GetSiteDomain(Site)}/dp/{Asin}"
        ListTaskUrl.append(itemTask)
    jsonData = json.dumps(ListTaskUrl, ensure_ascii=False)
    SaveAtMQ(jsonData)




