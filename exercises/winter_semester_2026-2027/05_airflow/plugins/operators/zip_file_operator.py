from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

import os
import gzip, shutil

class UnzipFileOperator(BaseOperator):

    template_fields = ('zip_file', 'extract_to')
    ui_color = '#b05b27'

    @apply_defaults
    def __init__(
            self,
            zip_file,
            extract_to,
            *args, **kwargs):
        """
        :param zip_file: file to unzip (including path to file)
        :type zip_file: string
        :param extract_to: where to extract zip file to
        :type extract_to: string
        """

        super(UnzipFileOperator, self).__init__(*args, **kwargs)
        self.zip_file = zip_file
        self.extract_to = extract_to

    def execute(self, context):

        self.log.info("UnzipFileOperator execution started.")
        
        self.log.info("Unzipping '" + self.zip_file + "' to '" + self.extract_to + "'.")

        with gzip.open(self.zip_file, 'r') as f_in, open(self.extract_to, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

        self.log.info("UnzipFileOperator done.")

