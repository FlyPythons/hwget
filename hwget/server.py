#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import json
import hashlib
import logging
import argparse

from hwget.base import OBS, Downloader


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


def do_download(cfg):
    """
    {
        "ak": "replace_with_your_ak",
        "sk": "replace_with_your_sk",
        "region": "replace_with_region",
        "bucket": "replace_with_your_bucket",
        "tasks": [task_file]

    }
    :param cfg: config file

    :return:
    """

    bucket = cfg["bucket"]
    downloader = Downloader()
    obs = OBS(
        ak=cfg["ak"], sk=cfg["sk"], region=cfg["region"]
    )
    tasks = cfg["tasks"]

    for task in tasks:
        _date, _uid = task.split("/")[:2]
        os.mkdir(_uid)
        log_path = os.path.join(_uid, "%s.log" % _uid)
        md5_path = os.path.join(_uid, "%s.md5" % _uid)
        md5_content = ""

        logging.basicConfig(
            level=logging.INFO,
            filename=log_path,
            format='%(asctime)s [%(levelname)s] %(message)s',
            datefmt='%d %b %Y %H:%M:%S'
        )
        task_file = os.path.join(_uid, "%s.cfg" % _uid)
        obs.download(bucket, task, task_file)
        v = read_cfg(task_file)[_uid]
        for url, out in zip(v["urls"], v["outs"]):
            file_path = os.path.join(_uid, out)
            target = "%s/%s/%s" % (_date, _uid, out)
            response = downloader.download(url, file_path)
            if not response:
                obs.upload(bucket, target, file_path)
                md5_content += "%s\t%s\n" % (create_md5(file_path), out)

        LOG.info("create md5")
        with open(md5_path, "w") as fh:
            fh.write(md5_content)
        obs.upload(bucket, "%s/%s/%s.md5" % (_date, _uid, _uid), md5_path)
        logging.shutdown()
        obs.upload(bucket, "%s/%s/%s.log" % (_date, _uid, _uid), log_path)


def add_args(parser):

    parser.add_argument("cfg", help="config")

    return parser


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""
    description:
    Server module
""")

    parser = add_args(parser)
    args = parser.parse_args()
    cfg = read_cfg(args.cfg)
    do_download(cfg)


if __name__ == "__main__":
    main()
