from selenium.webdriver.chrome.options import Options  # 无界面
from selenium.common.exceptions import TimeoutException
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver
from fateadm_api import TestFunc
import datetime
import requests
import pymssql
import logging
import time
import re
import os

import pprint


class MyViewSpider():
    def __init__(self):
        self.headers = {
            'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
        }
        logging.basicConfig(level=logging.WARNING,  # 控制台打印的日志级别
                    filename=f'{datetime.datetime.now().date()}new.log',
                    filemode='a',  ##模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志
                    # a是追加模式，默认如果不写的话，就是追加模式
                    format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')
        self.pattern_goodsStar = re.compile(r'\d+.\d+')
        self.pattern = re.compile(r'\d+')
        self.d = self.webdriverBasic()
        
        def webdriverBasic(self):
            chrome_options = Options()
            chrome_options.add_argument(
                '--user-agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"')  # 请求头
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('blink-settings=imagesEnabled=false')  # 不加载图片
            chrome_options.add_argument('lang=en-US.UTF-8')  # 修复美国站出现中文的问题
            chrome_options.add_argument('--headless')  # 隐藏界面
            chrome_options.add_argument('--disable-gpu')  # 谷歌文档提到需要加上这个属性来规避bug
            chrome_options.add_argument(
                "user-data-dir=C:\\Users\\Administrator\\AppData\Local\\Google\\Chrome\\User Data1")  # 使用这个 条件必定要隐藏界面 否则报错
            d = webdriver.Chrome(options=chrome_options)  # 生成无界面对象
            d.set_page_load_timeout(20)  # 设置最长等待时间
            d.maximize_window()
            return d
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
            
        def get_data(self, d, item, i=0):
            DataList = []
            goodsList = d.find_elements_by_xpath('.//div[@data-component-type="s-search-result"]')
            for goods in goodsList:
                ad = goods.find_elements_by_xpath('.//span[@class="a-size-mini a-color-secondary"]')  # 广告     是否考虑放在全局变量
                if not ad:
                    item_goods = {}
                    i += 1
                    try:
                        Link = goods.find_element_by_xpath('.//a[@class="a-link-normal a-text-normal"]').get_attribute(
                            'href')
                    except:
                        continue
                    try:
                        Title = goods.find_elements_by_xpath('.//a[@class="a-link-normal a-text-normal"]/span')[0].text
                    except:
                        Title = ""
                    try:
                        Star = goods.find_element_by_xpath('.//div[@class="a-row a-size-small"]/span').get_attribute(
                            'aria-label')
                        GoodsStar = self.pattern_goodsStar.findall(Star)[0]
                    except:
                        GoodsStar = ""
                    try:
                        CommentNum = goods.find_elements_by_xpath('.//span[@class="a-size-base"]')[0].text.replace(',', "")
                    except:
                        CommentNum = ""
                    try:
                        Price = goods.find_elements_by_xpath('.//a[@class="a-size-base a-link-normal a-text-normal"]/span')[
                            0].text.replace('\n', ".")
                    except:
                        Price = ""

                    item_goods['Ranking'] = str(i)
                    Asin = Link.split("/")[5]
                    if len(Asin) == 10:
                        item_goods["goodsAsin"] = Asin
                    else:
                        item_goods["goodsAsin"] = Link.split("/")[4]
                        if len(item_goods["goodsAsin"]) != 10:  # 如果以这种方式还抓不到ASIN则 这条数据不要
                            continue

                    item_goods['Title'] = Title.replace("'", "''")
                    if item["site"] != "JP":
                        item_goods['Brand'] = item_goods['Title'].split(" ")[0]
                    else:
                        item_goods['Brand'] = ""
                    item_goods['Star'] = GoodsStar
                    item_goods['CommentNum'] = CommentNum.replace(".", "")
                    item_goods['Price'] = Price
                    item_goods['goodsLink'] = Link
                    print(item_goods)
                    DataList.append(item_goods)
                else:
                    continue

            self.SaveAtDataDb(DataList, item, i)  # 第一时间入库





class HandleTask():
    def ReadDBTask(self):
        connect = pymssql.connect('192.168.2.163', 'sa', 'JcEbms123', 'EBMS')  # 服务器名,账户,密码,数据库名
        cursor = connect.cursor()  # 创建执行sql语句对象
        # select * from TbIndexReviewSpiderTask
        Sql = "select top 10 * from TbIndexReviewSpiderTask where taskState = 'New' Order By Id"  # 获取id   Top  每次拿出多少条
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


if __name__ == '__main__':
    Task = HandleTask()
    DbDataRows = Task.ReadDBTask()
    #尝试使用分组比如下面的3个一组
    # sortedGroup = Task.list_of_groups(DbDataRows,3)
    # pprint.pprint(sortedGroup)

    # print(DbDataRows)
    # (90187, 'CA', 'B005ISAOVY', None, 'New')

    ListTaskUrl = []  # 存放数据库任务
    for row in DbDataRows:
        item = {}
        item['taskid'] = row[0]
        item['taskurl'] = f'https://www.amazon.{row[1]}/dp/{row[2]}'
        ListTaskUrl.append(item)
    print(ListTaskUrl)

    print(len(ListTaskUrl))



