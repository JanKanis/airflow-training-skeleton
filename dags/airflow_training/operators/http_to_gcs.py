from tempfile import NamedTemporaryFile

from airflow import AirflowException
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.http_hook import HttpHook

from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults


class HttpToGcsOperator(BaseOperator):

    template_fields = ('endpoint', 'bucket', 'filename', 'data')

    @apply_defaults
    def __init__(self,
                 endpoint,
                 bucket,
                 filename,
                 method='GET',
                 data=None,
                 headers=None,
                 response_check=None,
                 extra_options=None,
                 http_conn_id='http_default',
                 log_response=False,

                 google_cloud_storage_conn_id="google_cloud_default",
                 delegate_to=None,

                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.endpoint = endpoint
        self.bucket = bucket
        self.filename = filename
        self.method = method
        self.data = data or {}
        self.headers = headers or {}
        self.response_check = response_check
        self.extra_options = extra_options or {}
        self.http_conn_id = http_conn_id
        self.log_response = log_response
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to



    def execute(self, context):
        http = HttpHook(self.method, http_conn_id=self.http_conn_id)

        self.log.info("Calling HTTP method")

        response = http.run(self.endpoint,
                            self.data,
                            self.headers,
                            self.extra_options)
        if self.log_response:
            self.log.info(f"HTTP {response.status_code} from {response.url}: {response.text}")

        if self.response_check:
            if not self.response_check(response):
                raise AirflowException("Response check returned False.")

        tmp_file_handle = NamedTemporaryFile(mode='w+t', delete=True)
        tmp_file_handle.write(response.text)


        self.log.info("Uploading to Google Cloud")
        gcs = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        gcs.upload(self.bucket, self.filename, tmp_file_handle.name, response.headers['content-type'])

        tmp_file_handle.close()








class LogWrapper:
     def __init__(self, logger):
         self.logger = logger

     def info(self, text):
         print(text)
         self.logger.info(text)