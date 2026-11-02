from airflow.plugins_manager import AirflowPlugin
from operators.copy_file_operator import *
from operators.create_directory_operator import *
from operators.clear_directory_operator import *

class FileSystemPlugin(AirflowPlugin):
    name = "filesystem_operations"
    operators = [CopyFileOperator, CreateDirectoryOperator, ClearDirectoryOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
