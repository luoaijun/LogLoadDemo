import datetime

import com.cdes
from cdes.service.log.web_log_offine import RUNOffline

if __name__ == '__main__':
    run = RUNOffline()
    startTime = datetime.datetime.now()
    run.sample_one_log(100000,10000000, 0)
    endTime = datetime.datetime.now()
    print("运行时间：" + str(endTime - startTime))

