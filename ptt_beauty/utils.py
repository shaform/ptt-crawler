import json
import os

from b2.upload_source import UploadSourceBytes
from scrapy.utils.project import get_project_settings


def get_shub_project_settings():
    settings = get_project_settings()
    shub_settings = json.loads(os.environ.get('SHUB_SETTINGS', '{}'))
    settings.setdict(
        shub_settings.get('project_settings', {}),
        priority='project')
    return settings


def split_bucket_prefix(uri):
    return uri[5:].split('/', 1)


def upload_file(bucket, file_name, content):
    source = UploadSourceBytes(content)
    bucket.upload(source, file_name)
