#!/usr/bin/env python
# -*- coding: utf-8 -*-
import queue
import random
import threading
import time

from hdfs import InsecureClient

from com.cdes.dao.DataWriterHdfs import DateUtilHdfs
from com.cdes.dao.DataWriterLocal import DateUtilLocal

exitFlag = 0
queueLock = threading.Lock()
threads = []
workQueue = queue.Queue(10000)
threadID = 1


class WebLogOffline(threading.Thread):

    def __init__(self, threadID, tName, q, index):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.tName = tName
        self.q = q
        self.index = index

    # 类属性，由所有类的对象共享
    site_url_base = "http://www.xxx.com/"

    def init(self):
        #  前面7条是IE,所以大概浏览器类型70%为IE ，接入类型上，20%为移动设备，分别是7和8条,5% 为空
        #  https://github.com/mssola/user_agent/blob/master/all_test.go
        self.user_agent_dist = {0.0: "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)",
                                0.1: "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)",
                                0.2: "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727)",
                                0.3: "Mozilla/4.0 (compatible; MSIE6.0; Windows NT 5.0; .NET CLR 1.1.4322)",
                                0.4: "Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko",
                                0.5: "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:41.0) Gecko/20100101 Firefox/41.0",
                                0.6: "Mozilla/4.0 (compatible; MSIE6.0; Windows NT 5.0; .NET CLR 1.1.4322)",
                                0.7: "Mozilla/5.0 (iPhone; CPU iPhone OS 7_0_3 like Mac OS X) AppleWebKit/537.51.1 (KHTML, like Gecko) Version/7.0 Mobile/11B511 Safari/9537.53",
                                0.8: "Mozilla/5.0 (Linux; Android 4.2.1; Galaxy Nexus Build/JOP40D) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.166 Mobile Safari/535.19",
                                0.9: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36",
                                1: " ", }
        self.ip_slice_list = [10, 29, 30, 46, 55, 63, 72, 87, 98, 132, 156, 124, 167, 143, 187, 168, 190, 201, 202, 214,
                              215, 222]
        self.url_path_list = ["login.php", "view.php", "list.php", "upload.php", "admin/login.php", "edit.php",
                              "index.html"]
        self.http_refer = ["http://www.baidu.com/s?q={query}", "http://www.google.cn/search?q={query}",
                           "http://www.sogou.com/web?q={query}", "http://one.cn.yahoo.com/s?q={query}",
                           "http://cn.bing.com/search?q={query}"]
        self.search_keyword = ["spark", "hadoop", "hive", "mysql", "mybatis", "spring", "springmvc"]

    def sample_ip(self):
        slice = random.sample(self.ip_slice_list, 4)  # 从ip_slice_list中随机获取4个元素，作为一个片断返回
        return ".".join([str(item) for item in slice])  # todo

    def sample_url(self):
        return random.sample(self.url_path_list, 1)[0]

    def sample_user_agent(self):
        dist_uppon = random.uniform(0, 1)
        return self.user_agent_dist[float('%0.1f' % dist_uppon)]

    # 主要搜索引擎referrer参数
    def sample_refer(self):
        if random.uniform(0, 1) > 0.7:  # 70% 流量有refer
            return "-"
        refer_str = random.sample(self.http_refer, 1)
        query_str = random.sample(self.search_keyword, 1)
        return refer_str[0].format(query=query_str[0])

    def run(self):
        self.process_data(self.tName, self.q)

    def process_data(self, threadName, q):
        client = InsecureClient("http://10.0.75.1:50070/", user="hdfs")

        global exitFlag, threadID, queueLock, workQueue, thread
        while not exitFlag:
            queueLock.acquire()
            if not workQueue.empty():
                queueLock.release()
                data = q.get()
                dateWriterHdfs = DateUtilHdfs()
                dataWriterLocal = DateUtilLocal()
                time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                query_log = "{ip} - - [{local_time}] \"GET /{url} HTTP/1.1\" 200 0 \"{refer}\" \"{user_agent}\" \"-\"".format(
                    ip=self.sample_ip(), local_time=time_str, url=self.sample_url(), refer=self.sample_refer(),
                    user_agent=self.sample_user_agent())
                # print(query_log)
                # print("%s processing %s" % (threadName, data))
                if self.index == 1:
                    dateWriterHdfs.getFileByDate(client, query_log + "\n",1)
                elif self.index == 0:
                    dataWriterLocal.getFileByDate(query_log + "\n")
            else:
                queueLock.release()
            time.sleep(1)


class RUNOffline:
    def sample_one_log(self, threadNum, count, index):
        global exitFlag, threadID, queueLock, workQueue, thread
        queueLock.acquire()
        for i in range(count):
            workQueue.put(i)
        queueLock.release()
        # 创建线程
        for tName in range(threadNum):
            thread = WebLogOffline(threadID, tName, workQueue, index)
            thread.init()
            thread.start()
            threads.append(thread)
            threadID += 1
            # 等待队列清空
        while not workQueue.empty():
            pass
            # 通知线程是时候退出
        exitFlag = 1

        # 等待所有线程完成
        for t in threads:
            t.join()
        print("退出主线程")
