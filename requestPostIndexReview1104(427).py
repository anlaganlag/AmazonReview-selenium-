from concurrent.futures import ThreadPoolExecutor
from lxml import etree
import requests
import datetime
import pymssql
import logging
import time
import os
import re
class IndexReviewSpider():
    def __init__(self):
        self.headers = {
            'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36",
        }
        logging.basicConfig(level=logging.WARNING, 
                            filename=f'{datetime.datetime.now().date()}new.log',
                            filemode='a',  
                            format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')


    def made_task(self, taskList):
        for task in taskList:
            item = {}
            item['taskid'] = task['taskid']
            item['taskLink'] = task['taskurl']  
            item['Asin'] = task['Asin']
            item['Site'] = task['Site']
            self.Scheduling_task(item)
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
            self.get_data(item, r.text)

        except Exception as e:
            print(f'任务分配错误:{e}--错误所在行数{e.__traceback__.tb_lineno}--地址:{item["taskLink"]}')
            logging.error(f'{e},错误所在行数{e.__traceback__.tb_lineno} --地址:{item["taskLink"]}')  # 将错误信息打印在控制台中

    def get_data(self, item, codeText):  #
        DataList = []
        try:
            myLIST=codeText.split("&&&")
            for msg in myLIST[3:-5]:
                item_reviews = {"Site":item["Site"],"Asin":item["Asin"],"CreateTime":item["CreateTime"]}
                itemObj=msg.replace("\n","")
                html_x = etree.HTML(itemObj)#解析成html
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
                    item_reviews ["ReviewDate"] = ""
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
                    ReviewTextList=html_x.xpath("//div[@data-hook='review-collapsed']/span/text()")
                    ReviewText="".join(ReviewTextList).replace('\\n', '').strip(),replace('\'','\^')
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


    def SaveAtDataDb(self, DataList, item):
        try:
            connect = pymssql.connect('192.168.2.163', 'sa', 'JcEbms123', 'EBMS')  # 服务器名,账户,密码,数据库名
            cursor = connect.cursor()  
        except Exception as e:
            print(f'数据库连接错误!{e},错误所在行数{e.__traceback__.tb_lineno} --地址:{item["taskLink"]}')
            logging.error(f'数据库连接错误!{e},错误所在行数{e.__traceback__.tb_lineno} --地址:{item["taskLink"]}')  # 将错误信息打印在控制台中
        try:
            DataSql =EndUpdateSql=Sql= ""
            item['taskEndTime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 任务结束时间
            EndUpdateSql = f"update TbIndexReviewSpiderTask set taskState='Success',SpiderTime='{item['taskEndTime']}' where CASIN='{item['Asin']}' and taskSite='{item['Site']}'".replace(u'\xa0', u' ')
            if not DataList:
                cursor.execute(EndUpdateSql)
            else:
                headSql = "INSERT INTO TbIndexReviewSpiderData ([ReviewId],[Site],[Asin],[CustomName],[ReviewStars],[ReviewTitle],[ReviewDate],[HelpfulNum],[ReviewText],[ReviewMedia],[CreateTime]) VALUES"
                for dictData in DataList:
                    DataSql += f" ('{dictData['ReviewId']}','{dictData['Site']}', '{dictData['Asin']}', '{dictData['CustomName']}', '{dictData['ReviewStars']}', '{dictData['ReviewTitle']}','{dictData['ReviewDate']}','{dictData['HelpfulNum']}', '{dictData['ReviewText']}', '{dictData['ReviewMedia']}', '{item['CreateTime']}'),"
                Sql = (headSql + DataSql).strip(",")
                cursor.execute(Sql)
                EndUpdateSql = f"update TbIndexReviewSpiderTask set taskState='Success',SpiderTime='{item['taskEndTime']}' where CASIN='{item['Asin']}' and taskSite='{item['Site']}'".replace(u'\xa0', u' ')
                cursor.execute(EndUpdateSql)
            connect.commit()
            connect.close()  # 关闭数据库
        except Exception as e:
            print(f'数据库存储失败: {e.__traceback__.tb_lineno}行代码出错! ({type(e).__name__+" "+ str(e.args[1])[2:70]}...)')
            logging.error(f'{e}\n,错误所在行数{e.__traceback__.tb_lineno}\n,Sql:\n{Sql}\n\n,EndUpdateSql:\n{EndUpdateSql}\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')  # 将错误信息打印在控制台中
    def GetSiteDomain(self,site):
        """从站点名获取站点对应的顶级域名"""
        siteDomain = {
                'US':"com",
                'UK':"co.uk"
        }
        return siteDomain[site] if siteDomain.get(site) else site.lower()
class getSQLData():
    def get_task(self):
        connect = pymssql.connect('192.168.2.163', 'sa', 'JcEbms123', 'EBMS')  # 服务器名,账户,密码,数据库名
        cursor = connect.cursor()  # 创建执行sql语句对象
        Sql = "select * from TbIndexReviewSpiderTask where taskState = 'New'"  # 获取id   Top  每次拿出多少条
        cursor.execute(Sql)
        rows = cursor.fetchall()
        connect.close()  # 关闭数据库
        return rows

    # 将单个列表 按照长度分割成多个列表
    def list_of_groups(self, list_info, per_list_len):
        '''
        :param list_info:   要分割的列表
        :param per_list_len:  每个小列表的长度
        :return:  end_list :   存分割之后的小列表的一个大列表
        '''
        list_of_group = zip(*(iter(list_info),) * per_list_len)
        end_list = [list(i) for i in list_of_group]  # i is a tuple
        count = len(list_info) % per_list_len
        end_list.append(list_info[-count:]) if count != 0 else end_list
        return end_list

    def GetSiteDomain(self,site):
        """从站点名获取站点对应的顶级域名"""
        siteDomain = {
                'US':"com",
                'UK':"co.uk"
        }
        return siteDomain[site] if siteDomain.get(site) else site.lower()
if __name__ == '__main__':
    start_time = time.perf_counter()
    SQLData = getSQLData()
    DbRows = SQLData.get_task()
    TaskList = []  # 存放数据库任务

    for taskid,Site,Asin,*rest in DbRows:
        task = {}
        task['taskid'] = taskid
        task['Site'] = Site
        task['Asin'] = Asin
        task['taskurl'] = f"https://www.amazon.{SQLData.GetSiteDomain(Site)}/dp/{Asin}"
        TaskList.append(task)



    if len(TaskList) != 0:
        if len(TaskList) < 20:
            threadsNumber = 1
        elif 20 <= len(TaskList) < 50:
            threadsNumber = 3
        else:
            threadsNumber = 8

        if len(TaskList) % threadsNumber == 0:  #
            groupOfTaskList = SQLData.list_of_groups(TaskList, len(TaskList) // threadsNumber)
        else:
            groupOfTaskList = SQLData.list_of_groups(TaskList,
                                                  (len(TaskList) // threadsNumber) + 1)  # 第二个参数是每个线程要执行的任务数
        print("开启的线程数目:",threadsNumber,"分割的列表个数:",len(groupOfTaskList),"第一个列表长度:",len(groupOfTaskList[0]))
        # 线程池
        with ThreadPoolExecutor(max_workers=threadsNumber) as t:  # 创建一个最大容纳数量为n的线程池
            spider = IndexReviewSpider()
            for i in range(0, threadsNumber):
                task = t.submit(spider.made_task, groupOfTaskList[i])

        logging.warning(f'首页评论爬虫任务已结束!!  本次任务数:{len(TaskList)} -- {datetime.datetime.now()}')
        end_time = time.perf_counter()
        logging.warning(f'总耗时{end_time - start_time}')
        logging.warning("============================END===================================")

    else:
        logging.basicConfig(level=logging.WARNING,  # 控制台打印的日志级别
                            filename=f'{datetime.datetime.now().date()}new.log',
                            filemode='a',  ##模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志
                            # a是追加模式，默认如果不写的话，就是追加模式
                            format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')
        logging.warning(f'首页评论爬虫任务已结束!!  本次任务数:{len(TaskList)} -- {datetime.datetime.now()}')
        logging.warning("============================END===================================")


