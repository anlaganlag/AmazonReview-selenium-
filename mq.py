from retry import retry
from pika.exceptions import AMQPConnectionError
from selenium.common.exceptions import TimeoutException
import pika
import datetime
import json
import requests
import logging

#获取站点后缀的help函数
def GetSiteDomain(self,site):
        """从站点名获取站点对应的顶级域名"""
        siteDomain = {
                'US':"com",
                'UK':"co.uk"
        }
        return siteDomain[site] if siteDomain.get(site) else site.lower()
#原来的消费需要改造成cb
def Scheduling_task(self, item):
    URL=f"https://www.amazon.{self.GetSiteDomain(item['Site'])}/hz/reviews-render/ajax/medley-filtered-reviews/get/ref=cm_cr_dp_d_fltrs_srt"
    body= {
        "asin": f"{item['Asin']}",
        "sortBy": "helpful",
        "scope": "reviewsAjax2",
    }
    try:
        item["CreateTime"] =  datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        r = requests.post(URL,headers=self.headers,data=body)
        # print(item,r.text)
        self.get_data(item, r.text)
    except Exception as e:
        print(f'任务分配错误:{e}--错误所在行数{e.__traceback__.tb_lineno}--地址:{item["taskLink"]}')
        logging.error(f'{e},错误所在行数{e.__traceback__.tb_lineno} --地址:{item["taskLink"]}')  # 将错误信息打印在控制台中

#连接mq
def conn(self):
    connection = pika.BlockingConnection(self.parameters)  # 创建连接
    connection.process_data_events()
    channel = connection.channel()  # 建立管道
    return channel

#消费端的mq 可以设置的是消费队列的名字 是否持久化  量 调用cb函数...
@retry(pika.exceptions.AMQPConnectionError, delay=5, jitter=(1, 3))   #MQ重连
def consumeMQ(self):
    channel = self.conn()
    channel.queue_declare(queue='AmazonReviewUrls', durable=True)  # 队列持久化
    channel.basic_qos(prefetch_count=100)  # 单个线程在MQ每次取得的消息量
    channel.basic_consume('AmazonReviewUrls', self.callback)  # 消费消息  如果收到消息就 调用回调函数
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()  #循环取任务


def consumeCallback(self, ch, method, properties, body):
    try:#解析的try
        listReview = json.loads(body)
        listReview["CreateTime"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        Site,Asin = listReview["Site"],listReview["Asin"]
        URL=f"https://www.amazon.{self.GetSiteDomain(Site)}/hz/reviews-render/ajax/medley-filtered-reviews/get/ref=cm_cr_dp_d_fltrs_srt"
        body= {
            "asin": f"{Asin}",
            "sortBy": "helpful",
            "scope": "reviewsAjax2",
        }
        try:#获取页面的try
            r = requests.post(URL,headers=self.headers,data=body)
        except TimeoutException:
            print(f'页面超时:{url}')
            logging.warning(f'页面超时:{url}')
            ch.basic_nack(delivery_tag=method.delivery_tag)  # 页面加载失败时，让该消息重回队列 ！！！
            # self.d.execute_script('window.stop()')  # 当页面加载时间超过设定时间，通过执行Javascript来停止页面加载，即可执行后续动作
            return
        self.get_data(item, r.text)#解析
            
        
    except pika.exceptions.AMQPConnectionError as e:  # 网络断线 无法连接MQ
        print('网络异常,MQ重连尝试重连...')
        logging.error('网络异常')

    except Exception as e:
        print(f'error:{e}')
        logging.error(f'error:{e},地址为:{url}')  # 将错误信息打印在控制台中
        ch.basic_nack(delivery_tag=method.delivery_tag)

def get_data(self, item, codeText):  #
    DataList = []
    try:
        myLIST=codeText.split("&&&")
        for msg in myLIST[3:-5]:
            item_reviews = {"Site":item["Site"],"Asin":item["Asin"],"CreateTime":item["CreateTime"]}
            itemObj=msg.replace("\n","")
            html=eval(itemObj)[2]#将json转换为python列表对象
            html_x = etree.HTML(html)#解析成html
            #获取评论Id
            try:
                ReviewId=html_x.xpath("//div[@class='a-section review aok-relative']/@id")[0]
                item_reviews ["ReviewId"] = ReviewId
            except:
                item_reviews ["ReviewId"] = ""
            #获取用户名
            try:
                CustomName=html_x.xpath("//span[@class='a-profile-name']/text()")[0].replace('\'','\^')
                item_reviews ["CustomName"] = CustomName
            except:
                item_reviews ["CustomName"] = ""
            # 获取评分
            try:
                ReviewStars= html_x.xpath("//span[@class='a-icon-alt']/text()")[0].split(" ")[0]
                item_reviews ["ReviewStars"] = ReviewStars.replace(",",".") 
            except:
                item_reviews ["ReviewStars"] = 0.0
            #获取评论标题
            try:
                ReviewTitle=html_x.xpath("//a[@data-hook='review-title']/span/text()")[0].replace('\'','\^')
                item_reviews ["ReviewTitle"] = ReviewTitle
            except:
                item_reviews ["ReviewTitle"] = ""
            #获取评论的日期
            try:
                ReviewDate=html_x.xpath("//span[@data-hook='review-date']/text()")[0]
                item_reviews ["ReviewDate"] = ReviewDate
            except:
                item_reviews ["ReviewDate"] = ReviewDate
            #获取有用数
            try:
                HelpfulNum=html_x.xpath("//span[@data-hook='helpful-vote-statement']/text()")[0].split(" ")[0]
                if len(HelpfulNum) > 1:#将one,或者Eine德语等转换成 1
                    HelpfulNum=1
                item_reviews ["HelpfulNum"] = HelpfulNum 
            except:
                item_reviews ["HelpfulNum"] = 0
            #获取评论正文
            try:
                ReviewText=html_x.xpath("//div[@data-hook='review-collapsed']/span/text()")[0].replace('\'','\^')
                item_reviews ["ReviewText"] = ReviewText
            except:
                item_reviews ["ReviewText"] = ""
            #获取评论图片
            try:
                ReviewMedia=html_x.xpath("//img[@class='cr-lightbox-image-thumbnail']/@src")
                item_reviews ["ReviewMedia"] = " ".join(ReviewMedia)
            except:
                item_reviews["ReviewMedia"] = ""
            DataList.append(item_reviews)

    except Exception as e:
        print(f'解析错误!{e},错误所在行数{e.__traceback__.tb_lineno} --地址:{item["taskLink"]}')
        logging.error(f'{e},错误所在行数{e.__traceback__.tb_lineno} --地址:{item["taskLink"]}')  # 将错误信息打印在控制台中
    print("尝试存入数据库的Asin和条数:",item['Asin'],len(DataList))
    self.SaveAtDataDb(DataList,item)
