# -*- coding: utf-8 -*-
import datetime


from cdes.service.log.web_log_stremging import WebLogStreming

if __name__ == '__main__':
    run = WebLogStreming()
    startTime = datetime.datetime.now()
    run.sample_one_log(10000000, 1)
    endTime = datetime.datetime.now()
    print("运行时间：" + str(endTime - startTime))
