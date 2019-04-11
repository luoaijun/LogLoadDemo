#!/usr/bin/python3
import datetime
import queue
import threading
import time

exitFlag = 0


class myThread(threading.Thread):
    def __init__(self, threadID, name, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q

    def run(self):
        # print("开启线程：" + self.name)
        process_data(self.name, self.q)
        # print("退出线程：" + self.name)


def process_data(threadName, q):
    while not exitFlag:
        queueLock.acquire()
        if not workQueue.empty():
            data = q.get()
            queueLock.release()
            print("%s processing %s" % (threadName, data))
        else:
            queueLock.release()
        time.sleep(1)


nameList = []
queueLock = threading.Lock()
workQueue = queue.Queue(1000)
threads = []
threadID = 1
if __name__ == '__main__':
    starttime = datetime.datetime.now()
    # 创建新线程
    for tName in range(1000):
        thread = myThread(threadID, tName, workQueue)
        thread.start()
        threads.append(thread)
        threadID += 1

    # 填充队列
    queueLock.acquire()
    for word in range(1000):
        workQueue.put(word)
    queueLock.release()

    # 等待队列清空
    while not workQueue.empty():
        pass

    # 通知线程是时候退出
    exitFlag = 1

    # 等待所有线程完成
    for t in threads:
        t.join()
    print("退出主线程")
    endtime = datetime.datetime.now()
    print("运行时间："+str(endtime-starttime))
