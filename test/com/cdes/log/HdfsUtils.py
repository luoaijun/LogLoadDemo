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
    # client = Client(url, root=None, proxy=None, timeout=None, session=None)
    # client = Client("http://hadoop:50070")
    # client = InsecureClient("http://10.0.75.1:50070/", user='hdfs')
    # # client = InsecureClient("http://120.78.186.82:50070", user='ann');
    hdfs = HDFileSystem(host="10.0.75.1", port='8020')
    path = "/data/"
    hdfs.cancel_token(token=None)  # 未知，求大佬指点
    hdfs.cat(path)  # 获取指定目录或文件的内容
    print(hdfs)
    print(hdfs.exists(path))
    # hdfs.chmod(path, mode)  # 修改制定目录的操作权限
    # hdfs.chown(path, owner, group)  # 修改目录所有者，以及用户组
    # hdfs.concat(destination,
    #             paths)  # 将指定多个路径paths的文件，合并成一个文件写入到destination的路径，并删除源文件（The source files are deleted on successful completion.成功完成后将删除源文件。）
    # hdfs.connect()  # 连接到名称节点 这在启动时自动发生。   LZ:未知作用，按字面意思，应该是第一步HDFileSystem(host='127.0.0.1', port=8020)发生的
    # hdfs.delegate_token(user=None)
    # hdfs.df()  # HDFS系统上使用/空闲的磁盘空间
    # hdfs.disconnect()  # 跟connect()相反，断开连接
    # hdfs.du(path, total=False, deep=False)  # 查看指定目录的文件大小，total是否把大小加起来一个总数，deep是否递归到子目录
    # hdfs.exists(path)  # 路径是否存在
    # hdfs.get(hdfs_path, local_path, blocksize=65536)  # 将HDFS文件复制到本地,blocksize设置一次读取的大小
    # hdfs.get_block_locations(path, start=0, length=0)  # 获取块的物理位置
    # hdfs.getmerge(path, filename, blocksize=65536)  # 获取制定目录下的所有文件，复制合并到本地文件
    # hdfs.glob(path)  # /user/spark/abc-*.txt 获取与这个路径相匹配的路径列表
    # hdfs.head(path, size=1024)  # 获取指定路径下的文件头部分的数据
    # hdfs.info(path)  # 获取指定路径文件的信息
    # hdfs.isdir(path)  # 判断指定路径是否是一个文件夹
    # hdfs.isfile(path)  # 判断指定路径是否是一个文件
    # hdfs.list_encryption_zones()  # 获取所有加密区域的列表
    # hdfs.ls(path, detail=False)  # 返回指定路径下的文件路径，detail文件详细信息
    # hdfs.makedirs(path, mode=457)  # 创建文件目录类似 mkdir -p
    # hdfs.mkdir(path)  # 创建文件目录
    # hdfs.mv(path1, path2)  # 将path1移动到path2
    # open(path, mode='rb', replication=0, buff=0, block_size=0)  # 读取文件，类似于python的文件读取
    # hdfs.put(filename, path, chunk=65536, replication=0, block_size=0)  # 将本地的文件上传到，HDFS指定目录
    # hdfs.read_block(fn, offset, length,
    #                 delimiter=None)  # 指定路径文件的offset指定读取字节的起始点，length读取长度，delimiter确保读取在分隔符bytestring上开始和停止
    # hdfs.read_block('/data/file.csv', 0, 13)
    # hdfs.read_block('/data/file.csv', 0, 13, delimiter=b'\n')
    # hdfs.rm(path, recursive=True)  # 删除指定路径recursive是否递归删除
    # hdfs.tail(path, size=1024)  # 获取 文件最后一部分的数据
    # hdfs.touch(path)  # 创建一个空文件
    # hdfs.walk(path)  # 遍历文件树
    # print(client)
    # hdfs.put_to_hdfs(client, 'F:\\Maven\\work\\CDES\\code\\LogDemo', '/data')