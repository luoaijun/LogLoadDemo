import time
import os.path

class DateUtilLocal:
    def getFileByDate(self,message):
        #获得当前系统时间的字符串
        localtime=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        # print('localtime='+localtime)
        #系统当前时间年份
        year=time.strftime('%Y',time.localtime(time.time()))
        #月份
        month=time.strftime('%m',time.localtime(time.time()))
        #日期
        day=time.strftime('%d',time.localtime(time.time()))
        #具体时间 小时分钟毫秒
        mdhms=time.strftime('%Y%m%d',time.localtime(time.time()))

        fileYear='F:/Maven/work/CDES/data/log/'+year
        fileMonth=fileYear+'/'+month
        fileDay=fileMonth+'/'+day

        if not os.path.exists(fileYear):
            os.mkdir(fileYear)
            os.mkdir(fileMonth)
            os.mkdir(fileDay)
        else:
            if not os.path.exists(fileMonth):
                os.mkdir(fileMonth)
                os.mkdir(fileDay)
            else:
                if not os.path.exists(fileDay):
                    os.mkdir(fileDay)

        #创建一个文件，以‘timeFile_’+具体时间为文件名称
        fileDir=fileDay+'/access_'+mdhms+'.txt'
        out=open(fileDir,'a+',encoding='utf-8')
        #在该文件中写入当前系统时间字符串
        out.write(message)
        out.close()
