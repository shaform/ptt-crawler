# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import functools
import logging

from io import BytesIO

from b2.api import B2Api
from b2.upload_source import UploadSourceBytes
from cryptography.fernet import Fernet
from twisted.internet import threads

from scrapy.pipelines.files import FilesPipeline
from scrapy.exceptions import NotConfigured, CloseSpider
from scrapy.utils.misc import load_object
from scrapy.utils.misc import md5sum
from scrapy.utils.python import without_none_values

logger = logging.getLogger(__name__)


class B2FilesStore(object):

    B2_ACCOUNT_ID = None
    B2_APPLICATION_KEY = None

    def __init__(self, uri):
        assert uri.startswith('b2://')
        self.bucket, self.prefix = uri[5:].split('/', 1)

        try:
            self.api = B2Api()
            self.api.authorize_account('production', self.B2_ACCOUNT_ID,
                                       self.B2_APPLICATION_KEY)
            self.c = self._get_b2_bucket()
        except Exception:
            raise CloseSpider('could not initialize B2')

    def stat_file(self, path, info):
        return {}

    def _get_b2_bucket(self):
        return self.api.get_bucket_by_name(self.bucket)

    def _upload_file(self, buf, file_name):
        source = UploadSourceBytes(buf.read())
        self.c.upload(source, file_name)

    def persist_file(self, path, buf, info, meta=None, headers=None):
        """Upload file to S3 storage"""
        key_name = '%s%s' % (self.prefix, path)
        buf.seek(0)

        return threads.deferToThread(self._upload_file,
                                     buf=buf,
                                     file_name=key_name)


class EncryptedFilesPipeline(FilesPipeline):
    def __init__(self, store_uri, download_func=None, settings=None):
        super().__init__(store_uri,
                         download_func=download_func,
                         settings=settings)

        cls_name = 'EncryptedFilesPipeline'
        resolve = functools.partial(self._key_for_pipe,
                                    base_class_name=cls_name,
                                    settings=settings)

        encryption_key = settings.get(resolve('FILES_ENCRYPTION_KEY'))
        if encryption_key is not None:
            self.cipher = Fernet(encryption_key)
        else:
            self.cipher = None

    @classmethod
    def from_settings(cls, settings):
        cls.STORE_SCHEMES = cls._load_components(settings, 'FILES_STORES')
        b2store = cls.STORE_SCHEMES['b2']
        b2store.B2_ACCOUNT_ID = settings['B2_ACCOUNT_ID']
        b2store.B2_APPLICATION_KEY = settings['B2_APPLICATION_KEY']
        store_uri = settings['FILES_STORE']
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

    def file_downloaded(self, response, request, info):
        path = self.file_path(request, response=response, info=info)
        if self.cipher is not None:
            buf = BytesIO(self.cipher.encrypt(response.body))
        else:
            buf = BytesIO(response.body)
        checksum = md5sum(buf)
        buf.seek(0)
        self.store.persist_file(path, buf, info)
        return checksum
