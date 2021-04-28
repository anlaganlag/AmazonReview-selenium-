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
import pymssql




class AmazonSpider():
    def __init__(self):
        self.headers = {
            'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
        }
        logging.basicConfig(level=logging.WARNING,  # 控制台打印的日志级别
                            filename=f'log\\{datetime.datetime.now().date()}new.log',
                            filemode='a',  ##模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志
                            # a是追加模式，默认如果不写的话，就是追加模式
                            format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')
        self.username = 'jc_crawler'  # 指定远程rabbitmq的用户名密码
        self.pwd = 'Jinchang001'
        self.user_pwd = pika.PlainCredentials(self.username, self.pwd)
        self.parameters = pika.ConnectionParameters('localhost',
                                                    credentials=self.user_pwd)

    def conn(self):
        connection = pika.BlockingConnection(self.parameters)  # 创建连接
        connection.process_data_events()
        channel = connection.channel()  # 建立管道
        return channel

    @retry(pika.exceptions.AMQPConnectionError, delay=5, jitter=(1, 3))   #MQ重连
    def taskScheduling(self):
        channel = self.conn()
        channel.queue_declare(queue='AmazonReviewUrls', durable=True)  # 队列持久化
        channel.basic_qos(prefetch_count=100)  # 单个线程在MQ每次取得的消息量
        channel.basic_consume('AmazonReviewUrls', self.callback)  # 消费消息  如果收到消息就 调用回调函数
        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()  #循环取任务

    def webdriverSingle(self):
        chrome_options = Options()
        chrome_options.add_argument(
            '--user-agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"')  # 请求头
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('blink-settings=imagesEnabled=false')  # 不加载图片
        chrome_options.add_argument('lang=en-US.UTF-8')  # 修复美国站出现中文的问题
        chrome_options.add_argument('--headless')  # 隐藏界面
        chrome_options.add_argument('--disable-gpu')  # 优化浏览器打开速度
        d = webdriver.Chrome(options=chrome_options)  # 生成无界面对象
        d.set_page_load_timeout(20)  # 设置最长等待时间
        d.maximize_window()
        return d

    def callback(self, ch, method, properties, body):
        # sleepStartTime = datetime.datetime.strptime('09:00:00', '%H:%M:%S')
        # sleepEndTime = datetime.datetime.strptime('18:30:00', '%H:%M:%S')
        # nowTime = datetime.datetime.strptime(datetime.datetime.now().strftime('%H:%M:%S'), '%H:%M:%S')
        #
        # if nowTime > sleepStartTime and nowTime < sleepEndTime:
        #     print("进入休眠状态，待18:30后继续工作")
        #     nowTimeSecond = int(nowTime.hour) * 3600 + int(nowTime.minute) * 60 + int(nowTime.second)
        #     sleepEndTimeSecond = int(sleepEndTime.hour) * 3600 + int(sleepEndTime.minute) * 60 + int(
        #         sleepEndTime.second)
        #     sleepSecond = sleepEndTimeSecond - nowTimeSecond
        #     print(f"休眠时间为：{sleepSecond}")
        #     time.sleep(sleepSecond)
        # # 爬虫程序在18：30-9：00间工作
        url = ""
        try:
            dictReview = {}
            jsonData = body
            # 将json数据格式 转成python数据格式
            listReview = json.loads(jsonData)
            url = listReview["Uri"]
            asin = listReview["Asin"]
            countryCode = listReview["Site"]
            dictReview["url"] = url
            dictReview["site"] = countryCode
            dictReview["asin"] = asin
            dictReview["taskGuid"] = listReview["TaskGuid"]

            listSpider = []  # 定义一个大列A表保存信息
            try:
                self.d.get(url)
            except TimeoutException:
                print(f'页面超时:{url}')
                logging.warning(f'页面超时:{url}')
                ch.basic_nack(delivery_tag=method.delivery_tag)  # 页面加载失败时，让该消息重回队列 ！！！
                # self.d.execute_script('window.stop()')  # 当页面加载时间超过设定时间，通过执行Javascript来停止页面加载，即可执行后续动作
                return
            item = {}  # 定义一个保存商品信息的字典
            page_count = self.d.page_source
            if len(page_count) >= 9000:
                try:
                    self.get_data(self.d, item, url, listSpider, dictReview, asin, countryCode)
                except Exception as e:
                    logging.error(f'解析模块异常:{e},异常行数:{e.__traceback__.tb_lineno},异常地址:{url}')
                    ch.basic_ack(delivery_tag=method.delivery_tag)  # 解析模块异常  消息重回队列
                    return
            elif len(page_count) > 6000 and len(page_count) < 9000:  # 处理验证码
                resNum = self.handle_Verification_Code(self.d, url, listSpider, item, dictReview, asin, countryCode)
                if resNum == 0:
                    ch.basic_nack(delivery_tag=method.delivery_tag)  # 验证码处理失败  消息重回队列
                    return

            else:  # 狗页面
                dictReview["DogOr404Page"] = True

            jsonData = json.dumps(dictReview, ensure_ascii=False)  # 将最后得到的储存所有数据的字典转成json
            self.SaveAtMQ(jsonData, ch, method)

        except pika.exceptions.AMQPConnectionError as e:  # 网络断线 无法连接MQ
            print('网络异常,MQ重连尝试重连...')
            logging.error('网络异常')

        except Exception as e:
            print(f'error:{e}')
            logging.error(f'error:{e},地址为:{url}')  # 将错误信息打印在控制台中
            ch.basic_nack(delivery_tag=method.delivery_tag)

    def SaveAtMQ(self, jsonData, ch, method):  # 写入MQ   若连接出现问题 会一直重复这里的操作
        connection = pika.BlockingConnection(self.parameters)  # 创建连接
        connection.process_data_events()  # 在执行长时间任务时，定时调用 process_data_events 方法，就不会丢失连接
        channel = connection.channel()  # 建立管道
        print(jsonData)
        channel.queue_declare(queue='AmazonReviewCrawler', durable=True)  # 是否队列持久化
        channel.basic_publish(exchange='',  # 交换机
                              routing_key='AmazonReviewCrawler',  # 路由键，写明将消息发往哪个队列
                              body=f'{jsonData}',
                              properties=pika.BasicProperties(
                                  delivery_mode=2, )  # delivery_mode=2 消息持久化
                              )  # 生产者要发送的消息
        ch.basic_ack(
            delivery_tag=method.delivery_tag)  # 1.消息生产者端发送消息时挂掉了,消费者接消息时挂掉了, rabbitMQ会让改消息重新回到消息队列中       2.手动向MQ确认消费
        connection.close()  # 当生产者发送完消息后，可选择关闭连接


if __name__ == '__main__':
    connect = pymssql.connect('192.168.2.163', 'sa', 'JcEbms123', 'EBMS')  # 服务器名,账户,密码,数据库名
    cursor = connect.cursor()  # 创建执行sql语句对象
    Sql = "select * from TbIndexReviewSpiderTask where taskState = 'New'"  # 获取id   Top  每次拿出多少条
    cursor.execute(Sql)
    rows = cursor.fetchall()
    connect.close()  # 关闭数据库


    for taskid,Site,Asin,*rest in DbDataRows:
        itemTask = {}
        itemTask['taskid'] = taskid
        itemTask['Site'] = Site
        itemTask['Asin'] = Asin
        itemTask['taskurl'] = f"https://www.amazon.{Task.GetSiteDomain(Site)}/dp/{Asin}"
        ListTaskUrl.append(itemTask)

    connection = pika.BlockingConnection(self.parameters)  # 创建连接
    connection.process_data_events()  # 在执行长时间任务时，定时调用 process_data_events 方法，就不会丢失连接
    channel = connection.channel()  # 建立管道
    channel.queue_declare(queue='IndexURL', durable=True)  # 是否队列持久化
    channel.basic_publish(exchange='',  # 交换机
                            routing_key='IndexURL',  # 路由键，写明将消息发往哪个队列
                            body=f'{jsonData}',
                            properties=pika.BasicProperties(
                                delivery_mode=2, )  # delivery_mode=2 消息持久化
                            )  # 生产者要发送的消息
    ch.basic_ack(
        delivery_tag=method.delivery_tag)  # 1.消息生产者端发送消息时挂掉了,消费者接消息时挂掉了, rabbitMQ会让改消息重新回到消息队列中       2.手动向MQ确认消费
    connection.close()  # 当生产者发送完消息后，可选择关闭连接
