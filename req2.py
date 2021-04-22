import requests
from lxml import etree

def cleanse(s):
    return s.replace('\\','')
URL = 'https://www.amazon.ca/dp/B00B0O1BBW'
URL2 = 'https://www.amazon.com/hz/reviews-render/ajax/medley-filtered-reviews/get/ref=cm_cr_dp_d_fltrs_srt'
URL3 = ' https://www.amazon.com/hz/reviews-render/ajax/medley-filtered-reviews/get/ref=cm_cr_dp_d_fltrs_srt'
body = {
    "language": "en_US",
    # "asin": "B07QQZDHMR",
    "asin": "B00B0O1BBW",
    "sortBy": "helpful",
    "scope": "reviewsAjax2",
}
headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
        }
r = requests.post(URL3,headers=headers,data=body)

r.encoding='utf-8'
json_text=r.text
myLIST=json_text.split("&&&")
for msg in myLIST[2:-5]:
    item=msg.replace("\n","")
    # 得到节点
    html=list(eval(item))[2]
    html_x = etree.HTML(html)
    try:
        # 获得评分
        pf= html_x.xpath("//span[@class='a-icon-alt']/text()")[0].split(" ")[0]
        # 评论者的用户名
        pr_name=html_x.xpath("//span[@class='a-profile-name']/text()")[0]
        print("    ",pr_name,"给出的评分是",pf)
    except:
        pass
