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






def ReadDBTask():
    connect = pymssql.connect('192.168.2.163', 'sa', 'JcEbms123', 'EBMS')  # 服务器名,账户,密码,数据库名
    cursor = connect.cursor()  # 创建执行sql语句对象
    Sql = "   select * from TbIndexReviewSpiderTask "
    # Sql = "   select top 750 rtLink,rtID from TbReptileTaskToEveryDay where rtType ='关键词爬虫' and rtState = '未完成' Order By rtLink"  # 获取id   Top  每次拿出多少条
    cursor.execute(Sql)
    rows = cursor.fetchall()
    connect.close()  # 关闭数据库
    return rows


if __name__ == '__main__':
    allData = ReadDBTask()
    print(len(allData),allData)