from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from hooks.hdfs_hook import HdfsHook


class HdfsGetFileOperator(BaseOperator):

    template_fields = ('remote_file', 'local_file', 'hdfs_conn_id')
    ui_color = '#fcdb03'

    @apply_defaults
    def __init__(
            self,
            remote_file,
            local_file,
            hdfs_conn_id,
            *args, **kwargs):
        """
        :param remote_file: file to download from HDFS
        :type remote_file: string
        :param local_file: where to store HDFS file locally
        :type local_file: string
        :param hdfs_conn_id: airflow connection id of HDFS connection to be used
        :type hdfs_conn_id: string
        """

        super(HdfsGetFileOperator, self).__init__(*args, **kwargs)
        self.remote_file = remote_file
        self.local_file = local_file
        self.hdfs_conn_id = hdfs_conn_id

    def execute(self, context):

        self.log.info("HdfsGetFileOperator execution started.")
        
        self.log.info("Download file '" + self.remote_file + "' to local filesystem '" + self.local_file + "'.")

        hh = HdfsHook(hdfs_conn_id=self.hdfs_conn_id)
        hh.getFile(self.remote_file, self.local_file)

        self.log.info("HdfsGetFileOperator done.")

