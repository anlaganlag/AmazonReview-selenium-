import requests
from lxml import etree

def cleanse(s):
    return s.replace('\\','')



def Scheduling_task(item,url):
    headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
    }
    # URL = 'https://www.amazon.ca/dp/B00B0O1BBW'
    # URL2 = 'https://www.amazon.com/hz/reviews-render/ajax/medley-filtered-reviews/get/ref=cm_cr_dp_d_fltrs_srt'
    # URL3 = ' https://www.amazon.com/hz/reviews-render/ajax/medley-filtered-reviews/get/ref=cm_cr_dp_d_fltrs_srt'
    headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
            }
    body = {
        "asin": f"{item['ASIN']}",
        "sortBy": "helpful",
        "scope": "reviewsAjax2",
    }
    r = requests.post(url,headers=headers,data=body)
    r.encoding='utf-8'
    json_text=r.text
    # resText = cleanse(res.text)
    myLIST=json_text.split("&&&")
    for msg in myLIST[3:-5]:
        itemObj=msg.replace("\n","")
        html=eval(itemObj)[2]
        html_x = etree.HTML(html)
        with open(f"{list(eval(itemObj))[2][9:18]}.txt","w") as f:
            f.write(etree.tostring(html_x).decode())
        try:
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
            print("    ","图片：",ReviewMedia)


        except Exception as inst:
            print(type(inst))    # the exception instance
            print(inst.args)     # arguments stored in .args
            print(inst)
url = 'https://www.amazon.com/hz/reviews-render/ajax/medley-filtered-reviews/get/ref=cm_cr_dp_d_fltrs_srt'
item={"ASIN":"B07J63HQ8W"}

Scheduling_task(item,url)


    # with open("test0422.txt","w") as f:
    #     f.write(etree.tostring(html_index).decode())

    # userName = html_index.xpath('//span[@class="a-profile-name"]/text()') #用户名
    # <a class="a-link-normal" title="4.0 out of 5 stars" 
    # stars = html_index.xpath('//a[@class="a-link-normal"]/@title')
    # title = html_index.xpath('//a[@data-hook="review-title"]/span/text()')
    # reviewDate = html_index.xpath('//span[@data-hook="review-date"]/text()')
    # <span data-hook=\"helpful-vote-statement\" class=\"a-size-base a-color-tertiary cr-vote-text\">4 people found this helpful</span>
    # helpful = html_index.xpath('//span[@data-hook="helpful-vote-statement"]/text()')
    # reviewContent = html_index.xpath('//div[@class="a-expander-content reviewText review-text-content a-expander-partial-collapse-content"]/span/text()')
    # reviewImage= html_index.xpath('//img[@data-hook="review-image-tile"]/@data-src')

    # print(userName)
    # print(stars)
    # print(title)
    # print(reviewDate)
    # print(helpful)
    # print(reviewContent)
    # print(reviewImage)
    # print(etree.tostring(html_index).decode())
    # title = titleEle.get_attribute("title")
    # print(titleEle)
    # print(res.text)
    # print(res.url)


    #总结下怎么抽revierw的数据
    #首先是通过review切换recent top找出url...
        # 确定了url后就是post获取数据
        # 需要提供了data等于body...