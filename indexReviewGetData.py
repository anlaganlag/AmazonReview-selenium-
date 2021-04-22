import requests
from lxml import etree

def cleanse(s):
    return s.replace('\\','')
def Scheduling_task(self, item, url):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
        }
        body = {
            #语言是可选的...
            # "language": "en_US",
            "asin": "B07QQZDHMR",
            #['recent',"reviewsAjax3"],['helpful',"reviewsAjax2"]
            "sortBy": "helpful",
            "scope": "reviewsAjax2",
            # "sortBy": "recent",
            # "scope": "reviewsAjax3",
        }
        try:
            item['taskStartTime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            res_item = requests.post(url, headers=headers,data=body)
            if len(res_item.text) > 10000:
                self.get_data(item, res_item.text)
            else:
                self.handle_Verification_Code(item)
        except Exception as e:
            print(f'error:{e}')
            logging.error(f'{e},错误所在行数{e.__traceback__.tb_lineno} --地址:{item["taskLink"]}')  # 将错误信息打印在控制台中

def get_data(self,item，codeText）：