import datetime

import com.cdes
from cdes.service.log.web_log_create import RUN
from cdes.service.log.web_log_stremging import WebLog

if __name__ == '__main__':
    # run = RUN()
    # startTime = datetime.datetime.now()
    # run.sample_one_log(100,100, 1)
    # endTime = datetime.datetime.now()
    # print("运行时间：" + str(endTime - startTime))
    run = WebLog()
    startTime = datetime.datetime.now()
    run.sample_one_log(100, 1)
    endTime = datetime.datetime.now()
    print("运行时间：" + str(endTime - startTime))
