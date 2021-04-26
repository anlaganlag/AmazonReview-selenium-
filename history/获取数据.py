from concurrent.futures import ThreadPoolExecutor
from fateadm_api import TestFunc
from selenium import webdriver
from lxml import etree
import requests
import datetime
import pymssql
import logging
import time
import os
import re
from lxml import etree
#简单的解析html数据..
class IndexSpider():
    def __init__(self):
        self.headers = {
            'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
        }
        logging.basicConfig(level=logging.WARNING,  # 控制台打印的日志级别
                            filename=f'{datetime.datetime.now().date()}new.log',
                            filemode='a',  ##模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志
                            # a是追加模式，默认如果不写的话，就是追加模式
                            format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')

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
    def GetSitePostfix(site):
        siteDict = {
                'US':"com",
                'UK':"co.uk"
        }
        if site in siteDict:
            return siteDict[site]
        return site.lower()  
    def Scheduling_task(self, item, url):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
        }
        body = {
        "asin": f"{item['Asin']}",
        "sortBy": "helpful",
        "scope": "reviewsAjax2",
        }
        URL=f"https://www.amazon.{GetSitePostfix(item['Site'])}/hz/reviews-render/ajax/medley-filtered-reviews/get/ref=cm_cr_dp_d_fltrs_srt"

        try:
            item["CreateTime"] =  datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            r = requests.post(URL,headers=headers,data=body)
            # print("=================全文",r.text)
            self.get_data(item, r.text)

        except Exception as e:
            print(f'安排任务错误:{e}')
            logging.error(f'{e},错误所在行数{e.__traceback__.tb_lineno} --地址:{item["taskLink"]}')  # 将错误信息打印在控制台中

    def get_data(self, item, codeText):  #
        DataList = []
        myLIST=codeText.split("&&&")
        for msg in myLIST[3:-5]:
            item_reviews = {"Site":item["Site"],"Asin":item["Asin"],"CreateTime":item["CreateTime"]}
            itemObj=msg.replace("\n","")
            html=eval(itemObj)[2]
            html_x = etree.HTML(html)
            try:
                try:
                    ReviewId=html_x.xpath("//div[@class='a-section review aok-relative']/@id")[0]
                    # print("     ReviewId是:",ReviewId)
                    item_reviews ["ReviewId"] = ReviewId
                except:
                    item_reviews ["ReviewId"] = ""
                try:
                    #获取用户名、
                    CustomName=html_x.xpath("//span[@class='a-profile-name']/text()")[0].replace('\'','\^')
                    item_reviews ["CustomName"] = CustomName
                except:
                    item_reviews ["CustomName"] = ""
                    #获取评分
                try:
                    ReviewStars= html_x.xpath("//span[@class='a-icon-alt']/text()")[0].split(" ")[0]
                    # print("    ",CustomName,"给出的评分是",ReviewStars)
                    item_reviews ["ReviewStars"] = ReviewStars.replace(",",".")
                except:
                    item_reviews ["ReviewStars"] =0.0
                try:
                    #获取标题
                    ReviewTitle=html_x.xpath("//a[@data-hook='review-title']/span/text()")[0].replace('\'','\^')
                    # print("    ","评论的标题是：",ReviewTitle)
                    item_reviews ["ReviewTitle"] = ReviewTitle
                except:
                    item_reviews ["ReviewTitle"] =""
                try:
                    #获取评论的日期
                    ReviewDate=html_x.xpath("//span[@data-hook='review-date']/text()")[0]
                    # print("    ","评论时间：",ReviewDate)
                    item_reviews ["ReviewDate"] = ReviewDate
                except:
                    item_reviews ["ReviewDate"] = ""
                    #获取有用数的日期
                try:
                    HelpfulNum=html_x.xpath("//span[@data-hook='helpful-vote-statement']/text()")[0].split(" ")[0]
                    # print("    ","好评数：",HelpfulNum)
                    if HelpfulNum == "one":
                        HelpfulNum=1
                    item_reviews ["HelpfulNum"] = HelpfulNum
                except:
                        item_reviews ["HelpfulNum"] = 0
                try:
                    #获取评论
                    ReviewText=html_x.xpath("//div[@data-hook='review-collapsed']/span/text()")[0].replace('\'','\^')
                    # print("    ","评论：",ReviewText)
                    item_reviews ["ReviewText"] = ReviewText
                except:
                    item_reviews ["ReviewText"] = "空"

                try:
                    #图片
                    ReviewMedia=html_x.xpath("//img[@class='cr-lightbox-image-thumbnail']/@src")
                    # print("    ","多媒体信息：",ReviewMedia)
                    if ReviewMedia:
                        item_reviews ["ReviewMedia"] = " ".join(ReviewMedia)
                except:
                    item_reviews["ReviewMedia"] = []
            
            except Exception as e:
                pass
                print(f'xpath解析错误:{e}')
                logging.error(f'{e},错误所在行数{e.__traceback__.tb_lineno} --地址:{item["taskLink"]}')  # 将错误信息打印在控制台中
            DataList.append(item_reviews)
        print("获取的结果条数:",item['Asin'],len(DataList))
        self.SaveAtDataDb(DataList,item)


    def handle_Verification_Code(self, dictData):
        d = self.webdriverSingle()
        while True:
            try:
                d.get(dictData["taskLink"])
                break
            except TimeoutException:
                print(f'页面超时:{dictData["taskLink"]}')
                logging.warning(f'页面超时:{dictData["taskLink"]},重新加载')
        if len(d.page_source) > 10000:
            self.get_data(dictData, d.page_source)
            d.quit()  # 一定要先拿数据再关闭
        else:
            print(f'页面被反爬,地址为:{dictData["taskLink"]}')
            logging.warning(f'进入验证码校验，源码量为{len(d.page_source)}，地址为:{dictData["taskLink"]} ')
            try:
                url_code = d.find_element_by_xpath('.//div[@class="a-row a-text-center"]/img').get_attribute('src')
            except:
                d.quit()
                self.handle_Verification_Code(dictData)
                return
            img = requests.get(url_code, headers=self.headers)
            imgTimeId = str(int(time.time() * 1000))
            with open(f'{dictData["taskLink"][-5:] + imgTimeId}img.jpg', 'wb') as f:
                f.write(img.content)
            time.sleep(1)
            code = TestFunc(dictData["taskLink"][-5:], imgTimeId)  # 拿到验证码
            # 找到验证码的页面输入验证码
            ifcaptchainput = d.find_elements_by_xpath('.//input[@id="captchacharacters"]')
            if len(ifcaptchainput) > 0:
                ifcaptchainput[0].send_keys(code)
            else:
                d.quit()
                self.handle_Verification_Code(dictData)
                return
            d.find_element_by_xpath('.//button[@class="a-button-text"]').click()
            time.sleep(2)
            page_count = d.page_source
            if len(page_count) > 6000 and len(page_count) < 9000:
                print("验证码解析失败")
                logging.warning(f'验证码解析失败:链接为{dictData["taskLink"]}')
                os.remove(f'{dictData["taskLink"][-5:] + imgTimeId}img.jpg')  # 验证失败  删除验证图片
                d.quit()
                self.handle_Verification_Code(dictData)
            else:
                print("验证码页面解析成功")
                logging.warning(f"验证码页面解析成功,Link:{dictData['taskLink']}")
                os.remove(f'{dictData["taskLink"][-5:] + imgTimeId}img.jpg')  # 删除验证码图片  预防不同进程间的错误识别
                self.get_data(dictData, d.page_source)
                d.quit()

    def SaveAtDataDb(self, DataList, item):
        try:
            connect = pymssql.connect('192.168.2.163', 'sa', 'JcEbms123', 'EBMS')  # 服务器名,账户,密码,数据库名
            cursor = connect.cursor()  # 创建执行sql语句对象
            headSql = "INSERT INTO TbIndexReviewSpiderData ([ReviewId],[Site],[Asin],[CustomName],[ReviewStars],[ReviewTitle],[ReviewDate],[HelpfulNum],[ReviewText],[ReviewMedia],[CreateTime]) VALUES"
            DataSql = ""
            for dictData in DataList:
                DataSql = f" ('{dictData['ReviewId']}','{dictData['Site']}', '{dictData['Asin']}', N'{dictData['CustomName']}', '{dictData['ReviewStars']}', N'{dictData['ReviewTitle']}','{dictData['ReviewDate']}','{dictData['HelpfulNum']}', '{dictData['ReviewText']}', '{dictData['ReviewMedia']}', '{item['CreateTime']}')," + DataSql
            Sql = (headSql + DataSql)[:-1]
            if len(Sql) < 200:
                return
            cursor.execute(Sql)    
            item['taskEndTime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 任务结束时间
            EndUpdateSql = f"update TbInd';exReviewSpiderTask set taskState='Success',SpiderTime='{item['taskEndTime']}' where CASIN={item['Asin']} and taskSite={item['Site']}"
            cursor.execute(EndUpdateSql)
            # print("EndUpdateSql:",EndUpdateSql)

            connect.commit()
            connect.close()  # 关闭数据库
                  
        except Exception as e:
            print(f'存储数据库错误:{e}')

            logging.error(f'{e},错误所在行数{e.__traceback__.tb_lineno} --地址:{item["taskLink"]}')  # 将错误信息打印在控制台中


if __name__ == '__main__':
    def GetSitePostfix(site):
        siteDict = {
                'US':"com",
                'UK':"co.uk"
        }
        if site in siteDict:
            return siteDict[site]
        return site.lower()  
    connect = pymssql.connect('192.168.2.163', 'sa', 'JcEbms123', 'EBMS')  # 服务器名,账户,密码,数据库名
    cursor = connect.cursor()  # 创建执行sql语句对象
    Sql = "select * from TbIndexReviewSpiderTask where taskState = 'New'"   #每次固定获取n行
    cursor.execute(Sql)
    rows = cursor.fetchall()
    connect.close()  # 关闭数据库
    # print(rows[72888:72889])
    ListTaskUrl = []  # 存放数据库任务

    for taskid,Site,Asin,StartScrping,state in rows:
        # print(taskid,Site,Asin,StartScrping,state,"===========6============")
        itemTask = {}
        itemTask['taskid'] = taskid
        itemTask['Asin'] = Asin
        itemTask['Site'] = Site
        itemTask['taskurl'] = f"https://www.amazon.{GetSitePostfix(Site)}/dp/{Asin}"
        ListTaskUrl.append(itemTask)
    print("任务列表:",ListTaskUrl)
    
    start_time = time.perf_counter()
    with ThreadPoolExecutor(max_workers=8) as t:  # 创建一个最大容纳数量为n的线程池
        spider = IndexSpider()
        for taskLink in ListTaskUrl:
            item = {}
            item['taskid'] = taskLink['taskid']
            item['taskLink'] = taskLink['taskurl']  # 直接拿到任务地址的索引
            item['Asin'] = taskLink['Asin']
            item['Site'] = taskLink['Site']
            print("任务的参数",item,"和链接",item['taskLink'])
            task = t.submit(spider.Scheduling_task, item, item['taskLink'])
    end_time = time.perf_counter()
    logging.warning(f'类目爬虫任务已结束!!  本次执行任务数:{len(ListTaskUrl)} -- {datetime.datetime.now()}')
    logging.warning(f'总耗时{end_time - start_time}')
    logging.warning("============================END===================================")
