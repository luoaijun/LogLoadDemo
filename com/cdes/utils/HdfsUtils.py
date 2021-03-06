# !coding:utf-8
import sys
from hdfs.client import Client, InsecureClient
from hdfs3 import HDFileSystem

# 关于python操作hdfs的API可以查看官网:
# https://hdfscli.readthedocs.io/en/latest/api.html

class HDFS:
    # 读取hdfs文件内容,将每行存入数组返回

    def read_hdfs_file(client, filename):
        # with client.read('samples.csv', encoding='utf-8', delimiter='\n') as reader:
        #  for line in reader:
        # pass
        lines = []
        with client.read(filename, encoding='utf-8', delimiter='\n') as reader:
            for line in reader:
                # pass
                # print line.strip()
                lines.append(line.strip())
        return lines

    # 创建目录
    def mkdirs(self, client, hdfs_path):
        client.makedirs(hdfs_path)

    def get(self, client, remotepath, localpath):
        client.get(remotepath, localpath)

    def put(self, client, localfile, remotefile):
        dir = self.getDirPath(remotefile)
        self.mkdir(dir)
        client.put(localfile, remotefile)

    def delete(self, client, remotepath):
        client.rm(remotepath, recursive=True)

    # 删除hdfs文件
    def delete_hdfs_file(self, client, hdfs_path):
        client.delete(hdfs_path)

    # 上传文件到hdfs
    def put_to_hdfs(client, local_path, hdfs_path):
        client.upload(hdfs_path, local_path, cleanup=True)

    # 从hdfs获取文件到本地
    def get_from_hdfs(client, hdfs_path, local_path):
        client.download(hdfs_path, local_path, overwrite=False)

    # 追加数据到hdfs文件
    def append_to_hdfs(client, hdfs_path, data):
        client.write(hdfs_path, data, overwrite=False, append=True, encoding='utf-8')

    # 覆盖数据写到hdfs文件
    def write_to_hdfs(client, hdfs_path, data):
        client.write(hdfs_path, data, overwrite=True, append=False, encoding='utf-8')

    # 移动或者修改文件
    def move_or_rename(client, hdfs_src_path, hdfs_dst_path):
        client.rename(hdfs_src_path, hdfs_dst_path)

    # 返回目录下的文件
    def list(client, hdfs_path):
        return client.list(hdfs_path, status=False)


def _main_(self):
    hdfs = HDFS()
    # client = Client(url, root=None, proxy=None, timeout=None, session=None)
    client = Client("http://hadoop:50070")
    client = InsecureClient("http://10.0.75.1:50070/", user='hdfs')
    print(client)
    hdfs.put_to_hdfs(client, 'F:\\Maven\\work\\CDES\\code\\LogDemo', '/data')