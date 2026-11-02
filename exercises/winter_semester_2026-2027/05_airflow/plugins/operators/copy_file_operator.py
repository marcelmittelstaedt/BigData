from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

import os
import shutil

class CopyFileOperator(BaseOperator):

    template_fields = ('source', 'dest', 'overwrite')
    ui_color = '#427bf5'

    @apply_defaults
    def __init__(
            self,
            source,
            dest,
            overwrite,
            *args, **kwargs):
        """
        :param source: source file
        :type source: string
        :param dest: destination file
        :type dest: string
        :param overwrite: overwrite file
        :type: overwrite: boolean
        """

        super(CopyFileOperator, self).__init__(*args, **kwargs)
        self.source = source
        self.dest = dest
        self.overwrite = overwrite

    def execute(self, context):

        self.log.info("CopyFileOperator execution started.")
        self.log.info("Overwrite: " + str(self.overwrite))
        
        if os.path.exists(self.dest) and self.overwrite == False:
            raise AirflowException("File '" + self.dest + "' already exists.")
        
        self.log.info("Copying file '" + self.source + "' to '" + self.dest + "'")
        
        dest = shutil.copyfile(self.source, self.dest) 

        self.log.info("CopyFileOperator done.")

