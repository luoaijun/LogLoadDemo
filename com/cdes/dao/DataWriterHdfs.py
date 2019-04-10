import time
import os.path
from os import path

from hdfs import InsecureClient, Client

from com.cdes.utils.HdfsUtils import HDFS
from hdfs3 import HDFileSystem

class DateUtilHdfs:
    def getFileByDate(self, message):
        client = InsecureClient("http://10.0.75.1:50070/",user="hdfs")
        hdfs = HDFS()

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

        fileYear = '/data/log/' + year
        fileMonth = fileYear + '/' + month
        fileDay = fileMonth + '/' + day

        hdfs.mkdirs(client, fileYear)
        hdfs.mkdirs(client, fileMonth)
        hdfs.mkdirs(client, fileDay)

        # 创建一个文件，以‘timeFile_’+具体时间为文件名称
        fileDir = fileDay + '/access_' + mdhms + '.log'
        fileLocal= path.dirname(path.dirname(__file__))+"\data"+'/access_' + mdhms + '.log'
        if not os.path.exists(fileLocal):
            # 在该文件中写入当前系统时间字符串
            out = open(fileLocal, 'a+', encoding='utf-8')
            # 在该文件中写入当前系统时间字符串
            out.write("")
            client.delete(fileDay)
            client.upload(fileDay, fileLocal, cleanup=True)
            out.close()
        client.write(fileDir, message, overwrite=False, append=True, encoding='utf-8')
        # client.append_to_hdfs(client, fileDir, message)



if __name__ == '__main__':
    client = InsecureClient("http://10.0.75.1:50070/", user='hdfs')
    print(client.checksum("/data/weibo/PurchaseRedemptionData/mfd_bank_shibor.csv"))
    print(client.list("/"))
    print(client.makedirs("/data/i"))

