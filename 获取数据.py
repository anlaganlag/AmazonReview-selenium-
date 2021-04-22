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
def get_simple_data(item,html):
    html_x = etree.HTML(html)
    #获取评论Id
    ReviewId=html_x.xpath("//div[@class='a-section review aok-relative']/@id")[0]
    print("     ReviewId是:",ReviewId)

    #获取用户名、
    CustomName=html_x.xpath("//span[@class='a-profile-name']/text()")[0]


    #获取评分
    ReviewStars= html_x.xpath("//span[@class='a-icon-alt']/text()")[0].split(" ")[0]
    print("    ",CustomName,"给出的评分是",ReviewStars)

    #获取标题
    ReviewTitle=html_x.xpath("//a[@data-hook='review-title']/span/text()")[0]
    print("    ","评论的标题是：",ReviewTitle)

    #获取评论的日期
    ReviewDate=html_x.xpath("//span[@data-hook='review-date']/text()")[0]
    print("    ","评论时间：",ReviewDate)

    #获取有用数的日期
    HelpfulNum=html_x.xpath("//span[@data-hook='helpful-vote-statement']/text()")[0].split(" ")[0]
    print("    ","好评数：",HelpfulNum)
    #获取评论
    ReviewText=html_x.xpath("//div[@data-hook='review-collapsed']/span/text()")[0]
    print("    ","评论：",ReviewText)
    #图片
    ReviewMedia=html_x.xpath("//img[@class='cr-lightbox-image-thumbnail']/@src")
    print("    ","多媒体信息：",ReviewMedia)

class CategorySpider():
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

    def Scheduling_task(self, item, url):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
        }
        body = {
        "asin": f"{item['ASIN']}",
        "sortBy": "helpful",
        "scope": "reviewsAjax2",
        }
        try:
            item['taskStartTime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            res_item = requests.get(url, headers=headers)
            if len(res_item.text) > 10000:
                self.get_data(item, res_item.text)
            else:
                self.handle_Verification_Code(item)
        except Exception as e:
            print(f'error:{e}')
            logging.error(f'{e},错误所在行数{e.__traceback__.tb_lineno} --地址:{item["taskLink"]}')  # 将错误信息打印在控制台中

    def get_data(self, item, codeText):  #
        DataList = []
        pattern_goodsStar = re.compile(r'\d+.\d+')
        if len(codeText) > 10000:
            html_index = etree.HTML(codeText)
            goodsList = html_index.xpath('.//li[@class="zg-item-immersion"]')

            cetegorys = html_index.xpath('.//li[@class="zg_browseUp"]/a/text()')
            cetegoryPath = ""
            for cetegory in cetegorys[1:]:  # [1:]跳过第一项
                cetegoryPath += cetegory + "/"
            cetegorySelected = html_index.xpath('.//span[@class="zg_selected"]/text()')
            if len(cetegorySelected) > 0:
                cetegorySelected = cetegorySelected[0].strip()
            else:
                cetegorySelected = ""
            item['cetegory'] = cetegoryPath + cetegorySelected

            item['site'] = self.GetSite(item['taskLink'])  # 判断站点

            for goods in goodsList:
                item_goods = {}
                goodsRanking = goods.xpath('.//span[@class="zg-badge-text"]/text()')[0].strip('#')
                try:
                    goodsTitle = goods.xpath('.//a[@class="a-link-normal"]/div/text()')[0].replace("\n", "").strip()
                except:
                    goodsTitle = ""
                try:
                    Link = goods.xpath('.//span[@class="aok-inline-block zg-item"]/a/@href')[0]
                except:
                    continue
                try:
                    goodsStar = goods.xpath('.//i/span[@class="a-icon-alt"]/text()')[0]
                    goodsStar = goodsStar.replace(',', '.')
                    goodsStar = pattern_goodsStar.findall(goodsStar)[0]
                except:
                    goodsStar = ""
                try:
                    goodsCommentNum = goods.xpath('.//a[@class="a-size-small a-link-normal"]/text()')[0].replace(
                        '.', '').replace(
                        ',', "").replace('\xa0', '')
                except:
                    goodsCommentNum = ""
                try:
                    goodsPriceList = goods.xpath('.//span[@class="p13n-sc-price"]/text()')
                    if len(goodsPriceList) == 1:
                        goodsPrice = goodsPriceList[0].replace('\xa0', '')
                    elif len(goodsPriceList) > 1:
                        goodsPrice = goodsPriceList[0].replace('\xa0', '') + " - " + goodsPriceList[1].replace('\xa0',
                                                                                                               '')
                    else:
                        goodsPrice = ""
                except:
                    goodsPrice = ""

                holdLink = item['taskLink']
                headLink = holdLink[:holdLink.index('/', 10)]
                goodsLink = headLink + Link
                item_goods['goodsTitle'] = goodsTitle.replace("'", "''")
                item_goods["site"] = item['site']
                if item_goods["site"] != "JP":
                    item_goods['goodsBrand'] = item_goods['goodsTitle'].split(" ")[0]
                else:
                    item_goods['goodsBrand'] = ""
                item_goods['cetegory'] = item['cetegory'].replace("'", "''")
                item_goods['goodsRanking'] = goodsRanking
                item_goods['goodsStar'] = goodsStar
                item_goods['goodsCommentNum'] = goodsCommentNum
                item_goods['goodsPrice'] = goodsPrice
                Asin = Link.split("/")[3]
                if len(Asin) == 10:
                    item_goods["goodsAsin"] = Asin
                else:
                    item_goods["goodsAsin"] = Link.split("/")[2]
                item_goods['goodsLink'] = goodsLink
                print(item_goods)
                DataList.append(item_goods)
            self.SaveAtDataDb(DataList, item)  # 每抓取一页数据就入爬虫数据库一次

            next_url = html_index.xpath('.//li[@class="a-last"]/a/@href')
            if len(next_url) > 0:
                next_url = next_url[0]
                item['taskLink'] = item['taskLink'] + '&pg=2'
                self.Scheduling_task(item, next_url)

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
        connect = pymssql.connect('192.168.0.228', 'sa', 'JcEbms123', 'EBMS')  # 服务器名,账户,密码,数据库名
        cursor = connect.cursor()  # 创建执行sql语句对象
        headSql = "INSERT INTO TbReptileDateToAmazonCategory ([site],[Ranking],[Asin],[Title],[Brand],[Cetegory],[Price],[CommentNum],[Star],[PageLink],[TaskLink]) VALUES"
        DataSql = ""
        for dictData in DataList:
            DataSql = f" ('{dictData['site']}','{dictData['goodsRanking']}', '{dictData['goodsAsin']}', N'{dictData['goodsTitle']}', '{dictData['goodsBrand']}', N'{dictData['cetegory']}','{dictData['goodsPrice']}','{dictData['goodsCommentNum']}', '{dictData['goodsStar']}', '{dictData['goodsLink']}', '{item['taskLink']}')," + DataSql
        Sql = (headSql + DataSql)[:-1]
        cursor.execute(Sql)
        if '&pg=2' not in item['taskLink']:  # 如果是第一页 那么我就把开始的时间写入
            StartUpdateSql = f"update TbReptileTaskToEveryDay set rtBegTime='{item['taskStartTime']}' where rtLink='{item['taskLink']}' and rtID={item['taskid']}"
            cursor.execute(StartUpdateSql)
        else:  # 就是第二页 我直接把结束的时间写入
            item['taskLink'] = item['taskLink'].replace("&pg=2", "")  # 先去除
            item['taskEndTime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 任务结束时间
            EndUpdateSql = f"update TbReptileTaskToEveryDay set rtState='已完成', rtEndTime='{item['taskEndTime']}' where rtLink='{item['taskLink']}' and rtID={item['taskid']}"
            cursor.execute(EndUpdateSql)
        connect.commit()
        connect.close()  # 关闭数据库

    def GetSite(self, url):
        if 'https://www.amazon.com/' in url:
            return 'US'
        elif 'https://www.amazon.co.uk/' in url:
            return 'UK'
        elif 'https://www.amazon.ca/' in url:
            return 'CA'
        elif 'https://www.amazon.fr/' in url:
            return 'FR'
        elif 'https://www.amazon.de/' in url:
            return 'DE'
        elif 'https://www.amazon.es/' in url:
            return 'ES'
        elif 'https://www.amazon.it/' in url:
            return 'IT'
        else:
            return 'JP'


def Scheduling_task(item,url):
    headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
    }

    body = {
        "asin": f"{item['ASIN']}",
        "sortBy": "helpful",
        "scope": "reviewsAjax2",
    }
    try:
        item["CreateTime"] =  datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        r = requests.post(url,headers=headers,data=body)
        r.encoding='utf-8'
        get_data(item, r.text)
    except Exception as e:
        print(f'error:{e}')
        logging.error(f'{e},错误所在行数{e.__traceback__.tb_lineno} --地址:{item["taskLink"]}')  # 将错误信息打印在控制台中
def get_data(self,item,codeText):
    DataList = []
    # resText = cleanse(res.text)
    myLIST=codeText.split("&&&")
    for msg in myLIST[3:-5]:
        itemObj=msg.replace("\n","")
        html=eval(itemObj)[2]
        html_x = etree.HTML(html)
        with open(f"{list(eval(itemObj))[2][9:18]}.txt","w") as f:
            f.write(etree.tostring(html_x).decode())
        try:
            #获取评论Id
            ReviewId=html_x.xpath("//span[@class='a-section review aok-relative']/@id")[0]
            print("    ",CustomName,"给出的评分是",ReviewStars)

            #获取用户名、
            CustomName=html_x.xpath("//span[@class='a-profile-name']/text()")[0]
            print("    ",CustomName,"给出的评分是",ReviewStars)

            
            #获取评分
            ReviewStars= html_x.xpath("//span[@class='a-icon-alt']/text()")[0].split(" ")[0]
            print("    ",CustomName,"给出的评分是",ReviewStars)

            #获取标题
            ReviewTitle=html_x.xpath("//a[@data-hook='review-title']/span/text()")[0]
            print("    ","评论的标题是：",ReviewTitle)

            #获取评论的日期
            ReviewDate=html_x.xpath("//span[@data-hook='review-date']/text()")[0]
            print("    ","评论时间：",ReviewDate)

            #获取有用数的日期
            HelpfulNum=html_x.xpath("//span[@data-hook='helpful-vote-statement']/text()")[0].split(" ")[0]
            print("    ","好评数：",HelpfulNum)
            #获取评论
            ReviewText=html_x.xpath("//div[@data-hook='review-collapsed']/span/text()")[0]
            print("    ","评论：",ReviewText)
            #图片
            ReviewMedia=html_x.xpath("//img[@class='cr-lightbox-image-thumbnail']/@src")
            print("    ","图片：",ReviewMedia)


        except Exception as inst:
            print(type(inst))    # the exception instance
            print(inst.args)     # arguments stored in .args
            print(inst)
# url = 'https://www.amazon.com/hz/reviews-render/ajax/medley-filtered-reviews/get/ref=cm_cr_dp_d_fltrs_srt'
# item={"ASIN":"B07J63HQ8W"}

    def SaveAtDataDb(self, DataList, item):
        connect = pymssql.connect('192.168.0.228', 'sa', 'JcEbms123', 'EBMS')  # 服务器名,账户,密码,数据库名
        cursor = connect.cursor()  # 创建执行sql语句对象
        headSql = "INSERT INTO TbReptileDateToAmazonCategory ([site],[Ranking],[Asin],[Title],[Brand],[Cetegory],[Price],[CommentNum],[Star],[PageLink],[TaskLink]) VALUES"
        DataSql = ""
        for dictData in DataList:
            DataSql = f" ('{dictData['site']}','{dictData['goodsRanking']}', '{dictData['goodsAsin']}', N'{dictData['goodsTitle']}', '{dictData['goodsBrand']}', N'{dictData['cetegory']}','{dictData['goodsPrice']}','{dictData['goodsCommentNum']}', '{dictData['goodsStar']}', '{dictData['goodsLink']}', '{item['taskLink']}')," + DataSql
        Sql = (headSql + DataSql)[:-1]
        cursor.execute(Sql)
        if '&pg=2' not in item['taskLink']:  # 如果是第一页 那么我就把开始的时间写入
            StartUpdateSql = f"update TbReptileTaskToEveryDay set rtBegTime='{item['taskStartTime']}' where rtLink='{item['taskLink']}' and rtID={item['taskid']}"
            cursor.execute(StartUpdateSql)
        else:  # 就是第二页 我直接把结束的时间写入
            item['taskLink'] = item['taskLink'].replace("&pg=2", "")  # 先去除
            item['taskEndTime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 任务结束时间
            EndUpdateSql = f"update TbReptileTaskToEveryDay set rtState='已完成', rtEndTime='{item['taskEndTime']}' where rtLink='{item['taskLink']}' and rtID={item['taskid']}"
            cursor.execute(EndUpdateSql)
        connect.commit()
        connect.close()  # 关闭数据库

if __name__ == '__main__':
    connect = pymssql.connect('192.168.2.163', 'sa', 'JcEbms123', 'EBMS')  # 服务器名,账户,密码,数据库名
    cursor = connect.cursor()  # 创建执行sql语句对象
    Sql = "select * from TbIndexReviewSpiderTask where taskState = 'New'"   #每次固定获取n行
    cursor.execute(Sql)
    rows = cursor.fetchall()
    connect.close()  # 关闭数据库

    ListTaskUrl = []  # 存放数据库任务
    for taskid,Site,Asin,StartScrping,state in rows:
        itemTask = {}
        itemTask['taskid'] = taskid
        itemTask['Asin'] = Asin
        itemTask['Site'] = Site
        itemTask['taskurl'] = f"https://www.amazon.{Site}/dp/{Asin}"
        ListTaskUrl.append(itemTask)
    start_time = time.perf_counter()
    with ThreadPoolExecutor(max_workers=8) as t:  # 创建一个最大容纳数量为n的线程池
        spider = CategorySpider()
        for taskLink in ListTaskUrl:
            item = {}
            item['taskid'] = taskLink['taskid']
            item['taskLink'] = taskLink['taskurl']  # 直接拿到任务地址的索引
            item['Asin'] = taskLink['Asin']
            item['Site'] = taskLink['Site']

            task = t.submit(spider.Scheduling_task, item, item['taskLink'])
    end_time = time.perf_counter()
    logging.warning(f'类目爬虫任务已结束!!  本次执行任务数:{len(ListTaskUrl)} -- {datetime.datetime.now()}')
    logging.warning(f'总耗时{end_time - start_time}')
    logging.warning("============================END===================================")
