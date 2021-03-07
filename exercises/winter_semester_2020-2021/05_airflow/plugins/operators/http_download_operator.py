from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

import os
import requests

class HttpDownloadOperator(BaseOperator):

    template_fields = ('download_uri', 'save_to')
    ui_color = '#26730a'

    @apply_defaults
    def __init__(
            self,
            download_uri,
            save_to,
            *args, **kwargs):
        """
        :param download_uri: http uri of file to download
        :type download_uri: string
        :param save_to: where to save file
        :type save_to: string
        """

        super(HttpDownloadOperator, self).__init__(*args, **kwargs)
        self.download_uri = download_uri
        self.save_to = save_to

    def execute(self, context):

        self.log.info("HttpDownloadOperator execution started.")
        
        self.log.info("Downloading '" + self.download_uri + "' to '" + self.save_to + "'.")

        try:
            r = requests.get(self.download_uri)  
        except requests.exceptions.RequestException as e:
            raise AirflowException("Failure, could not execute request. Exception: " + str(e))
        
        if r.status_code != 200:
            raise AirflowException("Failure, could not download file. HTTP Code: " + r.status_code)
        
        with open(self.save_to, 'wb') as f:
            f.write(r.content)

        self.log.info("HttpDownloadOperator done.")

