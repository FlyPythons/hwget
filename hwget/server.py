#!/usr/bin/env python

import os
import json
import hashlib
import logging
import argparse

from hwget.base import OBS, Downloader
from hwget import __author__, __email__, __version__


LOG = logging.getLogger(__name__)


def create_md5(file):

    r = hashlib.md5()
    with open(file, "rb") as fh:
        while True:
            d = fh.read(10*1024*1024)
            if not d:
                break
            r.update(d)

    return r.hexdigest()


def read_cfg(cfg):
    with open(cfg) as fh:
        r = json.loads(fh.read(), encoding="utf-8")

    return r


def do_download(config):
    """
    {
        "ak": "replace_with_your_ak",
        "sk": "replace_with_your_sk",
        "region": "replace_with_region",
        "task": {
            "replace_with_uid": {
                "date": "your_date",
                "urls": [],
                "outs": []
                }
        }
    }
    :param config: config file

    :return:
    """
    cfg = read_cfg(config)
    bucket = cfg["bucket"]
    downloader = Downloader()
    obs = OBS(
        ak=cfg["ak"], sk=cfg["sk"], region=cfg["region"]
    )
    task_dict = cfg["task"]

    for uid, v in task_dict.items():
        os.mkdir(uid)
        date = v["date"]
        obs.mkdir(bucket, "%s/%s/" % (date, uid))
        log_path = os.path.join(uid, "%s.log" % uid)
        md5_path = os.path.join(uid, "%s.md5" % uid)
        md5_content = ""

        logging.basicConfig(
            level=logging.INFO,
            filename=log_path,
            format='%(asctime)s [%(levelname)s] %(message)s',
            datefmt='%d %b %Y %H:%M:%S'
        )

        for url, out in zip(v["urls"], v["outs"]):
            file_path = os.path.join(uid, out)
            target = "%s/%s/%s" % (date, uid, out)
            response = downloader.download(url, file_path)
            if not response:
                obs.upload(bucket, file_path, target)
                md5_content += "%s\n%s\n" % (out, create_md5(file_path))

        LOG.info("create md5")
        with open(md5_path, "w") as fh:
            fh.write(md5_content)
        obs.upload(bucket, md5_path, "%s/%s/%s.md5" % (date, uid, uid))
        logging.shutdown()
        obs.upload(bucket, log_path, "%s/%s/%s.log" % (date, uid, uid))


def add_args(parser):

    parser.add_argument("cfg", help="config")

    return parser


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""
    description:

    version: %s
    contact:  %s <%s>\
        """ % (__version__, " ".join(__author__), __email__))

    parser = add_args(parser)
    args = parser.parse_args()

    with open(args.cfg) as fh:
        cfg = json.loads(fh.read(), encoding="utf-8")

    do_download(cfg)


if __name__ == "__main__":
    main()
