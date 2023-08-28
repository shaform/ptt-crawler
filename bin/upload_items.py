import argparse
import gzip
import io
import json
import logging
import os

import boto3
from botocore.config import Config
from scrapinghub import ScrapinghubClient

from ptt_crawler.utils import get_shub_project_settings
from ptt_crawler.utils import split_bucket_prefix
from ptt_crawler.utils import upload_file


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


def main(
    api_key,
    project_id,
    spider_name,
    b2_endpoint,
    b2_key_id,
    b2_application_key,
    b2_path,
    delete,
):
    bucket_name, root = split_bucket_prefix(b2_path)
    bucket = None

    client = ScrapinghubClient(api_key)
    project = client.get_project(project_id)
    for name in spider_name:
        spider = project.spiders.get(name)
        job_list = spider.jobs.list(state="finished")
        keys = []
        for job in job_list:
            if "items" in job and job["items"] > 0:
                keys.append(job["key"])

        if keys:
            if bucket is None:
                b2 = get_b2_resource(b2_endpoint, b2_key_id, b2_application_key)
                bucket = b2.Bucket(bucket_name)
            for key in keys:
                job = spider.jobs.get(key)
                if job:
                    out = io.BytesIO()

                    with gzip.GzipFile(fileobj=out, mode="w") as outfile:
                        for item in job.items.iter():
                            line = json.dumps(item) + "\n"
                            outfile.write(line.encode("utf8"))
                    content = out.getvalue()
                    file_name = os.path.join(
                        root, name, key.replace("/", "-") + ".jl.gz"
                    )
                    upload_file(bucket, file_name, content)

                    if delete:
                        job.delete()
                        logging.warning("job {} deleted".format(key))


def parse_args():
    settings = get_shub_project_settings()

    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument("--api-key", default=settings.get("SHUB_APIKEY"))
    parser.add_argument(
        "--project-id", type=int, default=settings.get("SHUB_PROJECT_ID")
    )
    parser.add_argument("--b2-endpoint", default=settings.get("B2_ENDPOINT"))
    parser.add_argument("--b2-key-id", default=settings.get("B2_KEY_ID"))
    parser.add_argument(
        "--b2-application-key", default=settings.get("B2_APPLICATION_KEY")
    )
    parser.add_argument("--b2-path", default=settings.get("ITEMS_STORE"))
    parser.add_argument("--delete", action="store_true")
    parser.add_argument("spider_name", nargs="+", help="Spider name to get info from.")
    args = parser.parse_args()

    assert args.b2_path.startswith("b2://")

    return args


if "__main__" == __name__:
    main(**vars(parse_args()))
