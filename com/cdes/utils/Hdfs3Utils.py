from hdfs3 import HDFileSystem
class HdfsUtils:
    hdfs =HDFileSystem(host='10.0.75.1', port=8020)
    def mkdir(self,remotepath):
        if not self.exists(remotepath):
            hdfs.mkdir(dir)

    def get(self,remotepath, localpath):
        if self.exists(remotepath):
            hdfs.get(remotepath, localpath)

    def put(self,localfile, remotefile):
        dir = self.getDirPath(remotefile)
        self.mkdir(dir)
        hdfs.put(localfile, remotefile)

    def exists(self,remotepath):
        return hdfs.exists(remotepath)

    def delete(self,remotepath):
        if self.exists(remotepath):
            hdfs.rm(remotepath, recursive=True)