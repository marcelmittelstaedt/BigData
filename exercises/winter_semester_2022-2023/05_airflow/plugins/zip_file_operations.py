from airflow.plugins_manager import AirflowPlugin
from operators.zip_file_operator import *

class ZipFilePlugin(AirflowPlugin):
    name = "zip_file_operations"
    operators = [UnzipFileOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
