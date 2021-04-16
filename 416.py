import pymssql
import pprint
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



