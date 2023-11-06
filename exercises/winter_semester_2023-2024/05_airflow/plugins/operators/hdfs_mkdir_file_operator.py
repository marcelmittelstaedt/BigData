from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from hooks.hdfs_hook import HdfsHook


class HdfsMkdirFileOperator(BaseOperator):

    template_fields = ('directory', 'hdfs_conn_id')
    ui_color = '#fcdb03'

    @apply_defaults
    def __init__(
            self,
            directory,
            hdfs_conn_id,
            *args, **kwargs):
        """
        :param directory: directory, which should be created
        :type directory: string
        :param hdfs_conn_id: airflow connection id of HDFS connection to be used
        :type hdfs_conn_id: string
        """

        super(HdfsMkdirFileOperator, self).__init__(*args, **kwargs)
        self.directory = directory
        self.hdfs_conn_id = hdfs_conn_id

    def execute(self, context):

        self.log.info("HdfsMkdirFileOperator execution started.")
        
        self.log.info("Mkdir HDFS directory'" + self.directory + "'.")

        hh = HdfsHook(hdfs_conn_id=self.hdfs_conn_id)
        hh.mkdir(self.directory)

        self.log.info("HdfsMkdirFileOperator done.")

