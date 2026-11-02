from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

import os
import glob

class ClearDirectoryOperator(BaseOperator):

    template_fields = ('directory', 'pattern')
    ui_color = '#427bf5'

    @apply_defaults
    def __init__(
            self,
            directory,
            pattern,
            *args, **kwargs):
        """
        :param directory: name of directory to clear files and directories within
        :type directory: string
        """

        super(ClearDirectoryOperator, self).__init__(*args, **kwargs)
        self.directory = directory
        self.pattern = pattern

    def execute(self, context):

        self.log.info("ClearDirectoryOperator execution started.")
        
        if os.path.exists(self.directory):
            if os.path.isdir(self.directory):
                if self.pattern == '':
                    raise AirflowException("Failure, file pattern is empty.")
                fileList = glob.glob(self.directory + '/' + self.pattern, recursive=True)
                for filePath in fileList:
                    try:
                        os.remove(filePath)
                        self.log.info("Deleted file '" + filePath + "'.")
                    except OSError:
                        raise AirflowException("Failure, couldn't delete file '" + filePath + "'.")
                if len(fileList) == 0:
                    self.log.info("No files to delete matching pattern '" + self.pattern  + "' found in directory '" + self.directory + "'.")
            else:
                raise AirflowException("Directory '" + self.directory + "' is not a directory.")
        else:
            raise AirflowException("Directory '" + self.directory + "' does not exist.")

        self.log.info("ClearDirectoryOperator done.")

