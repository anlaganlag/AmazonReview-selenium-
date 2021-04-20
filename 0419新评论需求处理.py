from selenium.webdriver.chrome.options import Options  # 无界面

from selenium.common.exceptions import TimeoutException
from concurrent.futures import ThreadPoolExecutor
from pika.exceptions import AMQPConnectionError
from selenium import webdriver
from fateadm_api import TestFunc
import datetime
import requests
import pymssql
import logging
import time
import re
import os

from retry import retry
#将数据推送到mq
import pika




class HandleTask():
    def ReadDBTask(self):
        connect = pymssql.connect('192.168.2.163', 'sa', 'JcEbms123', 'EBMS')  # 服务器名,账户,密码,数据库名
        cursor = connect.cursor()  # 创建执行sql语句对象
        Sql = "   select * from TbIndexReviewSpiderTask where taskState='New'"
        # Sql = "   select top 750 rtLink,rtID from TbReptileTaskToEveryDay where rtType ='关键词爬虫' and rtState = '未完成' Order By rtLink"  # 获取id   Top  每次拿出多少条
        cursor.execute(Sql)
        rows = cursor.fetchall()
        connect.close()  # 关闭数据库
        return rows
    
    def list_of_groups(self,list_info,per_list_len):
        '''
            ：param list_info: 要分割的列表
            ：param per_list_len:每个小列表的长度
            ：return end_list:分割后的的大列表包括小列表
        '''

        list_of_group = zip(*[iter(list_info)]*per_list_len)
        end_list = [list(i) for i in list_of_group]
        count = len(list_info) % per_list_len
        end_list.append(list_info[-count:]) if count != 0 else end_list
        return end_list


class IndexReviewSpider():
    def __init__(self):
        self.d = self.webdriverSingle()
        self.headers = {
            'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
        }
        logging.basicConfig(level=logging.WARNING,  # 控制台打印的日志级别
                            filename=f'{datetime.datetime.now().date()}new.log',
                            filemode='a',  ##模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志
                            # a是追加模式，默认如果不写的话，就是追加模式
                            format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')
        self.username = 'jc_crawler'  # 指定远程rabbitmq的用户名密码
        self.pwd = 'Jinchang001'
        self.user_pwd = pika.PlainCredentials(self.username, self.pwd)
        self.parameters = pika.ConnectionParameters('192.168.2.214',
                                                    credentials=self.user_pwd)  # heartbeat_interval=0  mq不断开连接
    def conn(self):
        connection = pika.BlockingConnection(self.parameters)  # 创建连接
        connection.process_data_events()
        channel = connection.channel()  # 建立管道
        return channel

    @retry(pika.exceptions.AMQPConnectionError, delay=5, jitter=(1, 3))
    def taskScheduling(self):  # 开启多个消费者
        channel = self.conn()
        channel.queue_declare(queue='AmazonReviewUrls', durable=True)  # 队列持久化
        channel.basic_qos(prefetch_count=100)  # 单个进程在MQ每次取得的消息量
        channel.basic_consume('AmazonReviewUrls', self.callback)  # 消费消息  如果收到消息就 调用回调函数      ,auto_ack=True  读取消息之后数据删除
        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()  # 只要一运行  就一直在等待

    def made_task(self, taskList):
        for task in taskList:
            item = {}
            item['taskid'] = task['taskid']
            item['taskLink'] = task['taskurl']  # 索引
            item["site"] = self.GetSite(item['taskLink'])
            self.Scheduling_task(item)

    def Scheduling_task(self, item, i=0):
        try:
            while True:
                try:
                    self.d.get(item['taskLink'])
                    break
                except TimeoutException:
                    print(f'页面超时:{item["taskLink"]}')
                    logging.warning(f'页面超时:{item["taskLink"]}')

            item['taskStartTime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            page_count = self.d.page_source
            if len(page_count) >= 10000:
                self.change_Postcode(self.d, item)  # 更换邮编
                self.get_data(self.d, item, i)
            else:
                res = self.handle_Verification_Code(self.d, item, i)
                if res == 0:  # 验证码页加载出错
                    self.Scheduling_task(item, i)

        except Exception as e:
            print(f'{e},地址:{item["taskLink"]}')
            logging.error(f'{e},错误所在行数{e.__traceback__.tb_lineno} --地址:{item["taskLink"]}')  # 列表超出索引  有可能是更换邮编时间过长导致


    def webdriverSingle(self):
        chrome_options = Options()
        chrome_options.add_argument(
            '--user-agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"')  # 请求头
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('blink-settings=imagesEnabled=false')  # 不加载图片
        chrome_options.add_argument('lang=en-US.UTF-8')  # 修复美国站出现中文的问题
        chrome_options.add_argument('--headless')  # 隐藏界面
        chrome_options.add_argument('--disable-gpu')  # 谷歌文档提到需要加上这个属性来规避bug
        d = webdriver.Chrome(options=chrome_options)  # 生成无界面对象
        d.set_page_load_timeout(20)  # 设置最长等待时间
        d.maximize_window()
        return d
    
    

if __name__ == '__main__':
    Task = HandleTask()

    DbDataRows = Task.ReadDBTask()
    
    ListTaskUrl = []  # 存放数据库任务
    # print(len(allData),allData)
    for row in DbDataRows:
        item = {}
        item['taskid'] = row[0]
        item['taskurl'] = f'https://www.amazon.{row[1]}/dp/{row[2]}'
        ListTaskUrl.append(item)
    # print(len(ListTaskUrl),ListTaskUrl[200:210])
    if len(ListTaskUrl) != 0:
        if len(ListTaskUrl) < 20:
            threadsNumber = 1
        elif 20 <= len(ListTaskUrl) < 50:
            threadsNumber = 3
        else:
            threadsNumber = 8

        if len(ListTaskUrl) % threadsNumber == 0:  #如果待处理的任务书能被线程整除
            excelData_group = Task.list_of_groups(ListTaskUrl, len(ListTaskUrl) // threadsNumber)
        else:
            excelData_group = Task.list_of_groups(ListTaskUrl, (len(ListTaskUrl) // threadsNumber) + 1)  # 第二个参数是每个线程要执行的任务数
        #线程池
        print(excelData_group)
        spiderList = []
        with ThreadPoolExecutor(max_workers=threadsNumber) as t:
            for i in range(0,threadsNumber):
                spider = IndexReviewSpider()
                locals()[i] = spider
                print(id(spider))
                spiderList.append(spider)
                task = t.submit(spider.made_task,excelData_group[i])
        for spiders in spiderList:
            spiders.d.quit()  # 依此关闭所有的浏览器


            logging.warning(f'关键词爬虫任务已结束!!  本次任务数:{len(ListTaskUrl)} -- {datetime.datetime.now()}')
            end_time = time.perf_counter()
            logging.warning(f'总耗时{end_time - start_time}')
            logging.warning("============================END===================================")

        else:
            logging.basicConfig(level=logging.WARNING,  # 控制台打印的日志级别
                                filename=f'{datetime.datetime.now().date()}new.log',
                                filemode='a',  ##模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志
                                # a是追加模式，默认如果不写的话，就是追加模式
                                format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')
            logging.warning(f'关键词爬虫任务已结束!!  本次任务数:{len(ListTaskUrl)} -- {datetime.datetime.now()}')
            logging.warning("============================END===================================")