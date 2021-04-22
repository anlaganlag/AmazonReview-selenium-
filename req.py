import requests
import json
from lxml import etree

def cleanse(s):
    return s.replace('\\','')
URL = 'https://www.amazon.ca/dp/B00B0O1BBW'
URL2 = 'https://www.amazon.com/hz/reviews-render/ajax/medley-filtered-reviews/get/ref=cm_cr_dp_d_fltrs_srt'
URL3 = ' https://www.amazon.com/hz/reviews-render/ajax/medley-filtered-reviews/get/ref=cm_cr_dp_d_fltrs_srt'
body = {
    "language": "en_US",
    "asin": "B07QQZDHMR",
    "sortBy": "helpful",
    "scope": "reviewsAjax2",
}
headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
        }
res = requests.post(URL3,headers=headers,data=body)
resText = cleanse(res.text)



html_index = etree.HTML(resText)
# with open("test0422.txt","w") as f:
#     f.write(etree.tostring(html_index).decode())

userName = html_index.xpath('//span[@class="a-profile-name"]/text()') #用户名
# <a class="a-link-normal" title="4.0 out of 5 stars" 
stars = html_index.xpath('//a[@class="a-link-normal"]/@title')
title = html_index.xpath('//a[@data-hook="review-title"]/span/text()')
reviewDate = html_index.xpath('//span[@data-hook="review-date"]/text()')
# <span data-hook=\"helpful-vote-statement\" class=\"a-size-base a-color-tertiary cr-vote-text\">4 people found this helpful</span>
helpful = html_index.xpath('//span[@data-hook="helpful-vote-statement"]/text()')
reviewContent = html_index.xpath('//div[@class="a-expander-content reviewText review-text-content a-expander-partial-collapse-content"]/span/text()')
reviewImage= html_index.xpath('//img[@data-hook="review-image-tile"]/@data-src')

# print(userName)
# print(stars)
# print(title)
# print(reviewDate)
# print(helpful)
# print(reviewContent)
print(reviewImage)
# print(etree.tostring(html_index).decode())
# title = titleEle.get_attribute("title")
# print(titleEle)
# print(res.text)
# print(res.url)


#总结下怎么抽revierw的数据
#首先是通过review切换recent top找出url...
    # 确定了url后就是post获取数据
    # 需要提供了data等于body...