import pymssql

connect = pymssql.connect('192.168.2.163', 'sa', 'JcEbms123',
                          'EBMS')  # 服务器名,账户,密码,数据库名
cursor = connect.cursor()  # 创建执行sql语句对象

#确认插入前存在
confirmSQL = f"select ReviewId from TbIndexReviewSpiderData where ReviewId = 'R3BFKCOND8W90P'"
cursor.execute(confirmSQL)

confirmSQLrows = cursor.fetchone()

print(confirmSQLrows)