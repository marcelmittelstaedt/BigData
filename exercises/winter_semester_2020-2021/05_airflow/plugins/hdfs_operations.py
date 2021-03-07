from airflow.plugins_manager import AirflowPlugin
from operators.hdfs_put_file_operator import *
from operators.hdfs_get_file_operator import *
from operators.hdfs_mkdir_file_operator import *
from hooks.hdfs_hook import *

class HdfsPlugin(AirflowPlugin):
    name = "hdfs_operations"
    operators = [HdfsPutFileOperator, HdfsGetFileOperator, HdfsMkdirFileOperator]
    hooks = [HdfsHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
