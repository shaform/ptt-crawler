import argparse
import gzip
import io
import json
import logging
import os

from b2.api import B2Api
from scrapinghub import ScrapinghubClient

from ptt_beauty.utils import get_shub_project_settings
from ptt_beauty.utils import split_bucket_prefix
from ptt_beauty.utils import upload_file


def main(api_key, project_id, spider_name, b2_account_id, b2_application_key,
         b2_path, delete):
    bucket_name, root = split_bucket_prefix(b2_path)
    bucket = None

    client = ScrapinghubClient(api_key)
    project = client.get_project(project_id)
    for name in spider_name:
        spider = project.spiders.get(name)
        job_list = spider.jobs.list(state='finished')
        keys = []
        for job in job_list:
            if 'items' in job and job['items'] > 0:
                keys.append(job['key'])

        if keys:
            if bucket is None:
                b2_api = B2Api()
                b2_api.authorize_account('production', b2_account_id,
                                         b2_application_key)
                bucket = b2_api.get_bucket_by_name(bucket_name)
            for key in keys:
                job = spider.jobs.get(key)
                if job:
                    out = io.BytesIO()

                    with gzip.GzipFile(fileobj=out, mode='w') as outfile:
                        for item in job.items.iter():
                            line = json.dumps(item) + '\n'
                            outfile.write(line.encode('utf8'))
                    content = out.getvalue()
                    file_name = os.path.join(root, name,
                                             key.replace('/', '-') + '.jl.gz')
                    upload_file(bucket, file_name, content)

                    if delete:
                        job.delete()
                        logging.warning('job {} deleted'.format(key))


def parse_args():
    settings = get_shub_project_settings()

    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument('--api-key', default=settings.get('SHUB_APIKEY'))
    parser.add_argument('--project-id',
                        type=int,
                        default=settings.get('SHUB_PROJECT_ID'))
    parser.add_argument('--b2-account-id',
                        default=settings.get('B2_ACCOUNT_ID'))
    parser.add_argument('--b2-application-key',
                        default=settings.get('B2_APPLICATION_KEY'))
    parser.add_argument('--b2-path', default=settings.get('ITEMS_STORE'))
    parser.add_argument('--delete', action='store_true')
    parser.add_argument('spider_name',
                        nargs='+',
                        help='Spider name to get info from.')
    args = parser.parse_args()

    assert args.b2_path.startswith('b2://')

    return args


if '__main__' == __name__:
    main(**vars(parse_args()))
