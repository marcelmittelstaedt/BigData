from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

import os

class CreateDirectoryOperator(BaseOperator):

    template_fields = ('path', 'directory')
    ui_color = '#427bf5'

    @apply_defaults
    def __init__(
            self,
            path,
            directory,
            *args, **kwargs):
        """
        :param path: path in which to create directory
        :type path: string
        :param directory: name of directory to create
        :type directory: string
        """

        super(CreateDirectoryOperator, self).__init__(*args, **kwargs)
        self.path = path
        self.directory = directory

    def execute(self, context):

        self.log.info("CreateDirectoryOperator execution started.")
        
        if not os.path.exists(self.path + '/' + self.directory):
            if os.path.isdir(self.path):
                try:
                    os.mkdir(self.path + '/' + self.directory)
                except OSError:
                    raise AirflowException("Creation of directory '" + self.path + "/" + self.directory + "' failed.")
                else:
                    self.log.info("Successfully created directory '" + self.path + "/" + self.directory + "'.")
            else:
                raise AirflowException("Path '" + self.path + "' is not a directory.")
        else:
            self.log.info("Directory '" + self.path + "/" + self.directory + "' already exists.")

        self.log.info("CreateDirectoryOperator done.")

