# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import os
import functools
import logging

from io import BytesIO

import boto3
from botocore.config import Config
from cryptography.fernet import Fernet
from scrapy.pipelines.files import FilesPipeline
from scrapy.exceptions import NotConfigured, CloseSpider
from scrapy.utils.misc import load_object
from scrapy.utils.misc import md5sum
from scrapy.utils.python import without_none_values
from twisted.internet import threads

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

logger = logging.getLogger(__name__)


def get_b2_resource(endpoint: str, key_id: str, application_key: str):
    b2 = boto3.resource(
        service_name="s3",
        endpoint_url=endpoint,  # Backblaze endpoint
        aws_access_key_id=key_id,  # Backblaze keyID
        aws_secret_access_key=application_key,  # Backblaze applicationKey
        config=Config(
            signature_version="s3v4",
        ),
    )
    return b2


class B2FilesStore(object):
    B2_ENDPOINT = None
    B2_KEY_ID = None
    B2_APPLICATION_KEY = None

    def __init__(self, uri: str) -> None:
        try:
            assert uri.startswith("b2://")
            buckets_and_prefix = uri[5:].split("/", 1)
            if len(buckets_and_prefix) > 1:
                self.bucket, self.prefix = buckets_and_prefix
            else:
                self.bucket = buckets_and_prefix[0]
                self.prefix = None

            self.b2 = get_b2_resource(
                endpoint=self.B2_ENDPOINT,
                key_id=self.B2_KEY_ID,
                application_key=self.B2_APPLICATION_KEY,
            )
            self.c = self._get_b2_bucket()
        except (AssertionError, Exception) as e:
            logger.exception(e)
            raise CloseSpider("could not initialize B2")

    def stat_file(self, path, info):
        return {}

    def _get_b2_bucket(self):
        return self.b2.Bucket(self.bucket)

    def _upload_file(self, buf, file_name):
        self.c.upload_fileobj(buf, file_name)

    def persist_file(self, path, buf, info, meta=None, headers=None):
        """Upload file to B2 storage"""
        if self.prefix:
            key_name = os.path.join(self.prefix, path)
        else:
            key_name = path
        buf.seek(0)

        return threads.deferToThread(self._upload_file, buf=buf, file_name=key_name)


class EncryptedFilesPipeline(FilesPipeline):
    def __init__(self, store_uri, download_func=None, settings=None):
        super().__init__(store_uri, download_func=download_func, settings=settings)

        cls_name = "EncryptedFilesPipeline"
        resolve = functools.partial(
            self._key_for_pipe, base_class_name=cls_name, settings=settings
        )

        encryption_key = settings.get(resolve("FILES_ENCRYPTION_KEY"))
        if encryption_key is not None:
            self.cipher = Fernet(encryption_key)
        else:
            self.cipher = None

    @classmethod
    def from_settings(cls, settings):
        cls.STORE_SCHEMES = cls._load_components(settings, "FILES_STORES")
        b2store = cls.STORE_SCHEMES["b2"]
        b2store.B2_ENDPOINT = settings["B2_ENDPOINT"]
        b2store.B2_KEY_ID = settings["B2_KEY_ID"]
        b2store.B2_APPLICATION_KEY = settings["B2_APPLICATION_KEY"]
        store_uri = settings["FILES_STORE"]
        return cls(store_uri, settings=settings)

    @staticmethod
    def _load_components(settings, setting_prefix):
        conf = without_none_values(settings.getwithbase(setting_prefix))
        d = {}
        for k, v in conf.items():
            try:
                d[k] = load_object(v)
            except NotConfigured:
                pass
        return d

    def file_downloaded(self, response, request, info, *, item=None):
        path = self.file_path(request, response=response, info=info)
        if self.cipher is not None:
            buf = BytesIO(self.cipher.encrypt(response.body))
        else:
            buf = BytesIO(response.body)
        checksum = md5sum(buf)
        buf.seek(0)
        self.store.persist_file(path, buf, info)
        return checksum
