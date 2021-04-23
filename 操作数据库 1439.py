import datetime
def SaveAtDataDb( DataList, item):
        headSql = "INSERT INTO TbIndexReviewSpiderData ([ReviewId],[Site],[Asin],[CustomName],[ReviewStars],[ReviewTitle],[ReviewDate],[HelpfulNum],[ReviewText],[ReviewMedia],[CreateTime]) VALUES"
        DataSql = ""
        for dictData in DataList:
            DataSql = f" ('{dictData['ReviewId']}','{dictData['Site']}', '{dictData['Asin']}', N'{dictData['CustomName']}', '{dictData['ReviewStars']}', N'{dictData['ReviewTitle']}','{dictData['ReviewDate']}','{dictData['HelpfulNum']}', '{dictData['ReviewText']}', '{dictData['ReviewMedia']}', '{item['CreateTime']}')," + DataSql
        Sql = (headSql + DataSql)[:-1]

        item['taskEndTime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 任务结束时间
        EndUpdateSql = f"update TbIndexReviewSpiderTask set taskState='已完成',SpiderTime='{item['taskEndTime']}' where CASIN='{item['Asin']}'"
        print(Sql,'\n',EndUpdateSql)


DataList=x2
item = {'taskid': 90394, 'taskLink': 'https://www.amazon.ca/dp/B00A9H3K7E', 'Asin': 'B00A9H3K7E', 'Site': 'CA', 'CreateTime': '2021-04-23 14:20:40'}
SaveAtDataDb(DataList,item)
