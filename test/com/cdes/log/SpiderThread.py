#!/usr/bin/python3

import threading
import time

exitFlag = 0
count = 0
class myThread (threading.Thread):
    def __init__(self, threadID, name, counter):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter = counter
    def run(self):
        global count
        print ("开始线程：" + self.name)
        print_time(self.name, self.counter,1)
        print(""+str(count))
        count = count+1
        print ("退出线程：" + self.name)

def print_time(threadName, delay, counter):
    while counter:
        if exitFlag:
            threadName.exit()
        time.sleep(delay)
        print ("%s: %s" % (threadName, time.ctime(time.time())))
        counter -= 1

# 创建新线程
for i in range(1,10):
    thread = myThread(i, "Thread-1", i)
    thread.start()
    thread.join()

print ("退出主线程")