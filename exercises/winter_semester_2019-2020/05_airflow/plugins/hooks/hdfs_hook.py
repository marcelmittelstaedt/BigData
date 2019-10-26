from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
import pyarrow as pa
import io

class HdfsHook(BaseHook):

    def __init__(self, hdfs_conn_id='hdfs_default'):
        self.hdfs_conn_id = hdfs_conn_id
        

    def get_conn(self):
        conn_data = self.get_connection(self.hdfs_conn_id)
        conn = pa.hdfs.connect(conn_data.host, conn_data.port, user=conn_data.login) 
        return conn

    def ls(self, directory):
        """
        List files within directory. Returns a list of items within given directory.

        :param directory: HDFS directory to do ls within
        :type directory: string
        """
        conn = self.get_conn()
        return conn.ls(directory)

    def putFile(self, local_file, remote_file):
        """
        Put local file to HDFS (hadoop fs -put...)

        :param local_file: file to upload
        :type local_file: string
        :param remote_file: target file on HDFS
        :type remote_file: string
        """
        conn = self.get_conn()
        with open(local_file, 'rb') as l_file:
            binary_data = io.BytesIO(l_file.read())

        conn.upload(remote_file, binary_data)


    def getFile(self, remote_file, local_file):
        """
        Get file from HDFS (hadoop fs -get...)

        :param remote_file: target file on HDFS
        :type remote_file: string
        :param local_file: file to upload
        :type local_file: string
        """
        conn = self.get_conn()
        out_buf = io.BytesIO()
        conn.download(remote_file, out_buf)
        with open(local_file, "wb") as outfile:
            outfile.write(out_buf.getbuffer())

    def mkdir(self, directory):
        """
        Create Directory within HDFS (hadoop fs -mkdir...)

        :param directory: directory to create within HDFS
        :type directory: string
        """        
        conn = self.get_conn()
        conn.mkdir(directory)

