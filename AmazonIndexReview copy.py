from concurrent.futures import ThreadPoolExecutor
from pika.exceptions import AMQPConnectionError
from lxml import etree
import requests
import datetime
import pymssql
import logging
import json
import time
import os
import re
from retry import retry
import pika


def GetSiteDomain(site):
    """从站点名获取站点对应的顶级域名"""
    siteDomain = {'US': "com", 'UK': "co.uk"}
    return siteDomain[site] if siteDomain.get(site) else site.lower()


class IndexReviewSpider():
    def __init__(self):
        self.headers = {
            'User-Agent':
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36",
        }
        logging.basicConfig(
            level=logging.WARNING,  # 控制台打印的日志级别
            filename=f'{datetime.datetime.now().date()}new.log',
            filemode='a',  ##模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志
            # a是追加模式，默认如果不写的话，就是追加模式
            format=
            '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
        )
        self.username = 'jc_crawler'  # 指定远程rabbitmq的用户名密码
        self.pwd = 'Jinchang001'
        self.user_pwd = pika.PlainCredentials(self.username, self.pwd)
        self.parameters = pika.ConnectionParameters('192.168.2.214',
                                                    credentials=self.user_pwd)
    def conn(self):
        connection = pika.BlockingConnection(self.parameters)
        connection.process_data_events()
        channel = connection.channel()
        return channel,connection
    def ReadDBTask(self):
        connect = pymssql.connect('192.168.2.163', 'sa', 'JcEbms123', 'EBMS')  # 服务器名,账户,密码,数据库名
        cursor = connect.cursor()  # 创建执行sql语句对象
        Sql = "SELECT * FROM TbIndexReviewSpiderTask where taskState= 'New'"  # 获取id   Top  每次拿出多少条
        cursor.execute(Sql)
        rows = cursor.fetchall()
        connect.close()  # 关闭数据库
        return rows
    def DBToMQ(self, jsonData):  # 写入MQ   若连接出现问题 会一直重复这里的操作
        listItem = json.loads(jsonData)
        channel,connection  = self.conn()
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
    def sendToMQ(self):
        DbDataRows = self.ReadDBTask()
        ListTaskUrl = []
        for taskid,Site,Asin,*rest in DbDataRows:
            itemTask = {}
            itemTask['taskid'] = taskid
            itemTask['Site'] = Site
            itemTask['Asin'] = Asin
            itemTask['taskurl'] = f"https://www.amazon.{GetSiteDomain(Site)}/dp/{Asin}"
            ListTaskUrl.append(itemTask)
        jsonData = json.dumps(ListTaskUrl, ensure_ascii=False)
        self.DBToMQ(jsonData)
    @retry(pika.exceptions.AMQPConnectionError, delay=5, jitter=(1, 3))  #MQ重连
    def MQComsumer(self):
        try:
            channel,_ = self.conn()
            channel.queue_declare(queue='IndexReview', durable=True)  # 队列持久化
            channel.basic_qos(prefetch_count=100)  # 单个线程在MQ每次取得的消息量
            channel.basic_consume('IndexReview',
                                  self.callback)  # 消费消息  如果收到消息就 调用回调函数
            print(' 开始消费mq的数据...')
            channel.start_consuming()  #循环取任务
        except Exception as e:

            print(f'MQ消费错误:{e}--错误所在行数{e.__traceback__.tb_lineno}--地址:')
            logging.error(
                f'{e},错误所在行数{e.__traceback__.tb_lineno} --地址:')  # 将错误信息打印在控制台中

    def callback(self, ch, method, properties, jsondata):
        item = json.loads(jsondata)
        URL = f"https://www.amazon.{GetSiteDomain(item['Site'])}/hz/reviews-render/ajax/medley-filtered-reviews/get/ref=cm_cr_dp_d_fltrs_srt"
        body = {
            "asin": f"{item['Asin']}",
            "sortBy": "helpful",
            "scope": "reviewsAjax2",
        }

        try:
            item["CreateTime"] = datetime.datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S')
            response = requests.post(URL, headers=self.headers, data=body,timeout=20)
            reviewText = response.text.split('&&&')
            if len(reviewText) == 8:  #即评论页为空,可以直接确认消费
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            self.get_data(item, reviewText[3:-5], ch, method)

        except Exception as e:
            print(f'连接失误任务重新排队:{e}--错误所在行数{e.__traceback__.tb_lineno}')
            logging.error(
                f'{e},错误所在行数{e.__traceback__.tb_lineno} ')  # 将错误信息打印在控制台中
            ch.basic_nack(delivery_tag=method.delivery_tag)

    def get_data(self, item, codeText, ch, method):  #
        try:
            for msg in codeText:
                item_reviews = {
                    "Site": item["Site"],
                    "Asin": item["Asin"],
                    'Taskid': item["taskid"],
                    "CreateTime": item["CreateTime"]
                }
                itemObj = msg.replace("\n", "")
                html = eval(itemObj)[2]  #将json转换为python列表对象
                html_x = etree.HTML(html)  #解析成html
                #获取评论Id
                try:
                    item_reviews["ReviewId"] = html_x.xpath(
                        "//div[@class='a-section review aok-relative']/@id")[0]
                except:
                    item_reviews["ReviewId"] = ""
                #获取用户名
                try:
                    item_reviews["CustomName"] = html_x.xpath(
                        "//span[@class='a-profile-name']/text()")[0].replace(
                            '\'', '\^')
                except:
                    item_reviews["CustomName"] = ""
                # 获取评分
                try:
                    item_reviews["ReviewStars"] = html_x.xpath(
                        "//span[@class='a-icon-alt']/text()")[0].split(
                            " ")[0].replace(",", ".")
                except:
                    item_reviews["ReviewStars"] = 0.0
                #获取评论标题
                try:
                    item_reviews["ReviewTitle"] = html_x.xpath(
                        "//a[@data-hook='review-title']/span/text()"
                    )[0].replace('\'', '\^')
                except:
                    item_reviews["ReviewTitle"] = ""
                #获取评论的日期
                try:
                    item_reviews["ReviewDate"] = html_x.xpath(
                        "//span[@data-hook='review-date']/text()")[0]
                except:
                    item_reviews["ReviewDate"] = ""
                #获取有用数
                try:
                    HelpfulNum = html_x.xpath(
                        "//span[@data-hook='helpful-vote-statement']/text()"
                    )[0].split(" ")[0]
                    if len(HelpfulNum) > 1:  #将one,或者Eine德语等转换成 1
                        HelpfulNum = 1
                    item_reviews["HelpfulNum"] = HelpfulNum
                except:
                    item_reviews["HelpfulNum"] = 0
                #获取评论正文
                try:
                    ReviewTextList = html_x.xpath(
                        "//div[@data-hook='review-collapsed']/span/text()")
                    ReviewText = "".join(ReviewTextList).replace(
                        '\\n', '').strip().replace('\'', ' ')
                    item_reviews["ReviewText"] = ReviewText[:2000]
                except:
                    item_reviews["ReviewText"] = ""
                #获取评论图片
                try:
                    ReviewMedia = html_x.xpath(
                        "//img[@class='cr-lightbox-image-thumbnail']/@src")
                    item_reviews["ReviewMedia"] = " ".join(ReviewMedia)
                except:
                    item_reviews["ReviewMedia"] = ""
            jsonData = json.dumps(item_reviews,
                                  ensure_ascii=False)  # 将最后得到的储存所有数据的字典转成json
            print(f'准备保存到MQ {jsonData[:80]}')
            self.SaveAtMQ(jsonData, ch, method)

        except Exception as e:
            print(f'解析错误!{e},错误所在行数{e.__traceback__.tb_lineno}')
            logging.error(
                f'{e},错误所在行数{e.__traceback__.tb_lineno}')  # 将错误信息打印在控制台中
        # print("尝试存入数据库的Asin和条数:", item['Asin'], len(DataList))

    def SaveAtMQ(self, jsonData, ch, method):
        connection = pika.BlockingConnection(self.parameters)  # 创建连接
        connection.process_data_events(
        )  # 在执行长时间任务时，定时调用 process_data_events 方法，就不会丢失连接
        channel = connection.channel()  # 建立管道
        channel.queue_declare(queue='IndexReviewData', durable=True)  # 是否队列持久化
        channel.basic_publish(
            exchange='',  # 交换机
            routing_key='IndexReviewData',  # 路由键，写明将消息发往哪个队列
            body=f'{json.dumps(jsonData)}',
            properties=pika.BasicProperties(delivery_mode=2,
                                            )  # delivery_mode=2 消息持久化
        )  # 生产者要发送的消息

        ch.basic_ack(
            delivery_tag=method.delivery_tag
        )  # 1.消息生产者端发送消息时挂掉了,消费者接消息时挂掉了, rabbitMQ会让改消息重新回到消息队列中       2.手动向MQ确认消费
        connection.close()  # 当生产者发送完消息后，可选择关闭连接

    def SaveAtDataDb(self, DataList, item, ch, method):
        try:
            connect = pymssql.connect('192.168.2.163', 'sa', 'JcEbms123',
                                      'EBMS')  # 服务器名,账户,密码,数据库名
            cursor = connect.cursor()

            try:
                now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                item['taskEndTime'] = now
                DataSql = ""
                InsertHeadSql = "INSERT INTO TbIndexReviewSpiderData ([ReviewId],[Site],[Asin],[CustomName],[ReviewStars],[ReviewTitle],[ReviewDate],[HelpfulNum],[ReviewText],[ReviewMedia],[CreateTime]) VALUES"
                EndUpdateSql = f"update TbIndexReviewSpiderTask set taskState='Success',SpiderTime='{now}' where id='{item['taskid']}'"
                if DataList:
                    for dictData in DataList:
                        confirmSQL = f"select ReviewId from TbIndexReviewSpiderData where ReviewId = '{dictData['ReviewId']}'"
                        #确认插入前存在
                        cursor.execute(confirmSQL)
                        confirmSQLrows = cursor.fetchone()
                        if confirmSQLrows:
                            updateSQL = f"update TbIndexReviewSpiderData set CreateTime='{now}' where ReviewId = '{dictData['ReviewId']}'"
                            cursor.execute(updateSQL)
                        else:
                            DataSql += f" ('{dictData['ReviewId']}','{dictData['Site']}', '{dictData['Asin']}', '{dictData['CustomName']}', '{dictData['ReviewStars']}', '{dictData['ReviewTitle']}','{dictData['ReviewDate']}','{dictData['HelpfulNum']}', '{dictData['ReviewText']}', '{dictData['ReviewMedia']}', '{item['CreateTime']}'),"
                sql = (InsertHeadSql + DataSql).strip(",")
                if len(sql) > len(InsertHeadSql):
                    cursor.execute(sql)
                cursor.execute(EndUpdateSql)

                connect.commit()
                ch.basic_ack(delivery_tag=method.delivery_tag)

                connect.close()  # 关闭数据库
            except Exception as e:
                print(
                    f'数据库存储失败: {e.__traceback__.tb_lineno}行代码出错! ({type(e).__name__} {str(e.args[1])[2:70]}  )'
                )
                logging.error(
                    f'{e}\n,错误所在行数{e.__traceback__.tb_lineno}\n\n\n,EndUpdateSql:\n{EndUpdateSql}\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++'
                )  # 将错误信息打印在控制台中
        except Exception as e:
            print(f'数据库连接错误!{e},错误所在行数{e.__traceback__.tb_lineno} ')
            logging.error(f'数据库连接错误!{e},错误所在行数{e.__traceback__.tb_lineno} '
                          )  # 将错误信息打印在控制台中


if __name__ == '__main__':
    if not os.path.exists('log'):
        os.makedirs('log')
        print("日志文件夹创建成功！")
    # pushSpider = IndexReviewSpider()
    # pushSpider.sendToMQ()
    threadsNumber = 8
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=threadsNumber) as t:
        for i in range(threadsNumber):
            spider = IndexReviewSpider()
            locals()[i] = spider
            task = t.submit(spider.MQComsumer)
    # spider = IndexReviewSpider()
    # spider.MQComsumer()
    print(
        '===================================end=================================='
    )
    end = time.perf_counter()
    print(f'耗时{end - start}')
