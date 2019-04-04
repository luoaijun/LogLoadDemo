import time
import os.path
from hdfs import InsecureClient

from com.cdes.utils.HdfsUtils import HDFS


class DateUtil:
    def getFileByDate(self, message):
        client = InsecureClient("http://10.0.75.1:50070/", user='hdfs')

        # 获得当前系统时间的字符串
        localtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        # print('localtime='+localtime)
        # 系统当前时间年份
        year = time.strftime('%Y', time.localtime(time.time()))
        # 月份
        month = time.strftime('%m', time.localtime(time.time()))
        # 日期
        day = time.strftime('%d', time.localtime(time.time()))
        # 具体时间 小时分钟毫秒
        mdhms = time.strftime('%Y%m%d', time.localtime(time.time()))

        fileYear = '/data/node/' + year
        fileMonth = fileYear + '/' + month
        fileDay = fileMonth + '/' + day
        hdfs = HDFS()
        if not hdfs.exists(client, fileYear):

            hdfs.mkdirs(client, fileYear)
            hdfs.mkdirs(client, fileMonth)
            hdfs.mkdirs(client, fileDay)
        else:
            if not hdfs.exists(client, fileMonth):
                os.mkdir(client, fileMonth)
                os.mkdir(client, fileDay)
            else:
                if not hdfs.exists(client, fileDay):
                    os.mkdir(client, fileDay)

        # 创建一个文件，以‘timeFile_’+具体时间为文件名称
        fileDir = fileDay + '/access_' + mdhms + '.log'

        out = open(fileDir, 'a+', encoding='utf-8')
        # 在该文件中写入当前系统时间字符串
        client.append_to_hdfs(fileDir,client,message)
        # out.write(message)
        out.close()


if __name__ == '__main__':
    utils = DateUtil()
    utils.getFileByDate("你好", )
