from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from hooks.hdfs_hook import HdfsHook


class HdfsPutFileOperator(BaseOperator):

    template_fields = ('local_file', 'remote_file', 'hdfs_conn_id')
    ui_color = '#fcdb03'

    @apply_defaults
    def __init__(
            self,
            local_file,
            remote_file,
            hdfs_conn_id,
            *args, **kwargs):
        """
        :param local_file: which file to upload to HDFS
        :type local_file: string
        :param remote_file: where on HDFS upload file to
        :type remote_file: string
        :param hdfs_conn_id: airflow connection id of HDFS connection to be used
        :type hdfs_conn_id: string
        """

        super(HdfsPutFileOperator, self).__init__(*args, **kwargs)
        self.local_file = local_file
        self.remote_file = remote_file
        self.hdfs_conn_id = hdfs_conn_id

    def execute(self, context):

        self.log.info("HdfsPutFileOperator execution started.")
        
        self.log.info("Upload file '" + self.local_file + "' to HDFS '" + self.remote_file + "'.")

        hh = HdfsHook(hdfs_conn_id=self.hdfs_conn_id)
        hh.putFile(self.local_file, self.remote_file)

        self.log.info("HdfsPutFileOperator done.")

