# -*- coding:utf-8 -*-

import os
import time
import json
import logging
import binascii
import requests
from datetime import datetime
from collections import OrderedDict

from openstack import connection
from obs import *

LOG = logging.getLogger(__name__)


class Cloud(object):
    """
    create, search ECS related service
    """
    CLOUD = "myhuaweicloud.com"

    def __init__(self, ak, sk, region, project_id):
        """

        :param ak: 账号名
        :param sk: 密码
        :param region: 账号ID
        :param project_id: 项目ID
        """
        self.connect = self._connect(ak, sk, region, project_id)

    def _connect(self, ak, sk, region, project_id):

        LOG.info("Connect to Huawei Cloud.")

        try:
            conn = connection.Connection(
                cloud=self.CLOUD,
                ak=ak,
                sk=sk,
                region=region,
                project_id=project_id
            )
            LOG.info("Connect success.")
        except Exception as e:
            LOG.error("Connect fail.")
            LOG.error(e)
            raise Exception(e)

        return conn

    @property
    def vpc(self):
        """

        :return: dict vpc {name: id}
        """

        r = {i.name: i.id for i in self.connect.vpcv1.vpcs()}
        LOG.info("VPC: %s" % r)

        return r

    @property
    def subnet(self):
        """

        :return: dict subnet {name: id}
        """

        r = {i.name: i.id for i in self.connect.vpcv1.subnets()}
        LOG.info("Subnet: %s" % r)

        return r

    def available_zones(self):
        """
        region下可用zone
        :return: list zone
        """

        r = [i.name for i in self.connect.compute.availability_zones()]
        LOG.info("Zone: %s" % r)

        return r

    def available_flavors(self, zone):
        """
        zone下可用实例
        :param zone:
        :return: list flavor
        """
        r = [i.name for i in self.connect.ecs.flavors(availability_zone=zone)]
        LOG.info("Flavors in %s: %s" % (zone, r))
        return r

    def get_zone_has_flavor(self, flavor):
        """
        实例名查询可用zone
        :param flavor:
        :return: zone or None
        """
        LOG.info("Looking for zone has flavor %r" % flavor)
        for zone in self.available_zones():
            if flavor in self.available_flavors(zone):
                LOG.info(zone)
                return zone

        LOG.info("Not found")
        return None

    def create_service(self, name, flavor, root_gb, image, personality, user_data):
        """
        创建ECS服务器
        :param name: 名称
        :param flavor: 实例类型
        :param root_gb: 系统盘大小 GB
        :param image: 镜像ID
        :param personality: personality属性 {"path": "", "content": ""}
        :param user_data: user_data
        :return: server id
        """

        content = binascii.b2a_base64(personality["content"].encode())[:-1].decode("utf-8")
        user_data = binascii.b2a_base64(user_data.encode())[:-1].decode("utf-8")
        zone = self.get_zone_has_flavor(flavor)

        if zone is None:
            e = "Flavor %r not found."
            LOG.error(e)
            raise Exception(e)

        vpc = self.vpc["vpc-default"]
        subnet = self.subnet["subnet-default"]

        data = {
            "availability_zone": zone,
            "name": name,
            "imageRef": image,
            "root_volume": {
                "volumetype": "SATA",
                "size": root_gb
            },
            "personality": [
                {
                    "path": personality["path"],
                    "contents": content
                }
            ],
            "user_data": user_data,
            "adminPass": "Python123456",
            "flavorRef": flavor,
            "vpcid": vpc,
            "nics": [
                {
                    "subnet_id": subnet
                }
            ],
            "publicip": {
                "eip": {
                    "iptype": "5_bgp",
                    "bandwidth": {
                        "size": 5,
                        "sharetype": "PER",
                        "charge_mode": "bandwidth"
                    }
                }
            },
            "count": 1
        }

        LOG.info("Create ECS server %s. flavor:%s, root_size: %s Gb" % (name, flavor, root_gb))
        action = self.connect.ecs.create_server_ext(**data)
        server_id = action.server_ids[0]
        LOG.info("Job submit. server_id: %r, job_id:%s" % (server_id, action.job_id))
        job = self.wait_for_job(action.job_id, times=5, interval=20)

        server = self.show_server(server_id)
        if job.status == "SUCCESS":
            LOG.info("Create ECS server success. server_id: %s, status: %s" % (server_id, server.status))
        else:
            LOG.info("Create ECS server failed. server_id: %s, status: %s" % (server_id, server.status))

        return server_id

    def wait_for_job(self, job_id, times=10, interval=20):
        job = None
        for index in range(times):
            time.sleep(interval)
            job = self.connect.ecs.get_job(job_id)
            if job.status == "SUCCESS":
                print("Get job success after %s tries" % index)
                break
            elif job.status == "FAIL":
                print("Get job failed after %s tries" % index)
                break
            else:
                pass

        if job.status == "RUNNING":
            LOG.error("Job %r is still running" % job_id)
            raise Exception(job)

        return job

    def get_servers_after_job(self, job):

        sub_jobs = job.entities["sub_jobs"]
        success_servers = []
        failed_servers = []

        if len(sub_jobs) > 0:
            for sub_job in sub_jobs:
                if "server_id" in sub_job.get("entities"):
                    if sub_job["status"] == "SUCCESS":
                        success_servers.append(sub_job.get("entities").get("server_id"))
                    else:
                        failed_servers.append(sub_job.get("entities").get("server_id"))

        return success_servers, failed_servers

    def delete_server(self, server):

        if isinstance(server, str):
            uid = server
        else:
            uid = server.id

        data = {
            "servers": [
                {
                    "id": uid
                },
            ],
            "delete_publicip": True,
            "delete_volume": True
        }

        LOG.info("Delete ECS server. server_id: %s" % uid)
        action = self.connect.ecs.delete_server(**data)
        job = self.wait_for_job(action.job_id)
        success_servers, failed_servers = self.get_servers_after_job(job)

        if uid in success_servers:
            LOG.info("Delete ECS server success. server_id: %s" % uid)
        else:
            e = "Delete ECS server failed. server_id: %s" % uid
            LOG.error(e)
            raise Exception(e)

        return 0

    def show_server(self, uid):

        server = self.connect.compute.get_server(uid)

        return server


class OBS(object):
    """
    work with obs storage
    """
    OBS_URL_PATTERN = "https://obs.{region}.myhuaweicloud.com/"

    def __init__(self, ak, sk, region):
        """

        :param ak:
        :param sk:
        :param region:
        """
        self.region = region
        self.connect = self._connect(ak, sk, region)

    def _connect(self, ak, sk, region):
        server = self.OBS_URL_PATTERN.format(region=region)
        return ObsClient(access_key_id=ak, secret_access_key=sk, server=server)

    def mkdir(self, bucket, folder):
        if not folder.endswith("/"):
            folder += "/"
        resp = self.connect.putContent(bucket, folder, '')

        if resp.status < 300:
            LOG.info(resp.header)
        else:
            e = "Can not mkdir %s in bucket %s" % (folder, bucket)
            LOG.error(e)
            raise Exception(e)

        return folder

    def ls(self, bucket, folder):

        resp = self.connect.listObjects(bucket, prefix=folder)
        if resp.status < 300:
            r = OrderedDict()
            for content in resp.body.contents:
                r[content.key] = {"etag": content.etag, "size": content.content_length}

            LOG.info(r)
        else:
            LOG.error(resp.header)
            raise Exception(resp.header)

        return r

    def _upload(self, bucket, object_name, part_num, upload_id, file_path, part_size, offset):

        response = self.connect.uploadPart(
            bucket, object_name, part_num, upload_id, content=file_path, partSize=part_size, offset=offset,
            isFile=True, isAttachMd5=True)

        if response.status < 300:
            etag = response.body.etag
            LOG.info("%r part%s %s-%s success, etag: %s." % (file_path, part_size, part_size + offset, part_num, etag))
            return etag
        else:
            LOG.info("%r part%s %s-%s failed." % (file_path, part_size, part_size + offset, part_num))
            LOG.error(response.errorMessage)
            return None

    def upload(self, bucket, file, target, part_size=20*1024*1024):
        """

        :param bucket:
        :param file:
        :param target:
        :param part_size:
        :return:
        """
        LOG.info("Upload %r to %r" % (file, (bucket + "/" + target)))
        resp = self.connect.initiateMultipartUpload(bucket, target)
        if resp.status >= 300:
            LOG.error('initiateMultipartUpload failed')
            return 1
        upload_id = resp.body.uploadId

        file_size = os.path.getsize(file)
        LOG.info("File size: %s Gb" % (file_size/1024/1024/1024))
        part_num = int(file_size / part_size) if (file_size % part_size == 0) else int(file_size / part_size) + 1
        LOG.info("%r split into %s parts to upload" % (file, part_num))
        if part_num > 10000:
            LOG.error('Total parts count should not exceed 10000')
            return 1

        etag_dict = {}
        for i in range(part_num):
            offset = i * part_size
            curr_size = (file_size - offset) if i + 1 == part_num else part_size
            etag = self._upload(bucket, target, i + 1, upload_id, file, curr_size, offset)
            if etag is not None:
                etag_dict[i + 1] = etag

        parts = []

        for k, v in sorted(etag_dict.items(), key=lambda d: d[0]):
            parts.append(CompletePart(partNum=k, etag=v))

        resp = self.connect.completeMultipartUpload(bucket, target, upload_id, CompleteMultipartUploadRequest(parts))
        if resp.status < 300:
            LOG.info('Upload to %s success.' % target)
            return target
        else:
            return None


class Downloader(object):

    HEADER = {
        'User-Agent': 'user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36'
    }

    def _download(self, url, start, end, out):

        header = self.HEADER.update({"Range": "bytes=%s-%s" % (start, end)})
        response = requests.get(url, headers=header, stream=True)
        LOG.info("Download %r from %s to %s" % (url, start, end))
        with open(out, 'ab') as fh:
            for chunk in response.iter_content(chunk_size=1024*1024):
                if chunk:
                    fh.write(chunk)

        return 0

    def download(self, url, out, retry=5):

        response = requests.get(url, headers=self.HEADER, stream=True)
        file_size = int(response.headers['content-length'])

        LOG.info("Download %r to %s" % (url, out))
        n = 0
        while True:
            if os.path.exists(out):
                download_size = os.path.getsize(out)
            else:
                download_size = 0

            if download_size >= file_size:
                LOG.info("%s download success" % url)
                return 0

            if n >= retry:
                LOG.error("%s download failed" % url)
                return 1

            self._download(url, download_size, file_size, out)

            n += 1


class Hwget(object):

    def __init__(self, ak, sk, region, project_id, bucket, image):

        self.ak = ak
        self.sk = sk
        self.region = region
        self.project_id = project_id
        self.bucket = bucket
        self.image = image

        self.cloud = Cloud(
            ak=ak, sk=sk, region=region, project_id=project_id
        )

        self.obs = OBS(
            ak=ak, sk=sk, region=region
        )

    @staticmethod
    def _get_content_size(url):
        resp = requests.get(url, stream=True)
        if resp.status_code == 200 and 'Content-Length' in resp.headers:
            return int(resp.headers['Content-Length'])
        else:
            return 0

    @staticmethod
    def _get_disk_size_gb(size):
        boot_gb = 2
        n = (size/1024.0/1024/1024 + boot_gb) / 10.0
        if n < 5:
            n = 4

        return (n + 1) * 10

    @staticmethod
    def _generate_id(urls):
        text = "|".join(sorted(urls))

        import hashlib
        return hashlib.md5(text.encode("utf-8")).hexdigest()

    def _check_files_exists_in_obs(self, bucket, folder, files):
        if folder[-1] != "/":
            folder += "/"

        r = []
        files_exists = self.obs.ls(bucket, folder)

        for f in files:
            if folder+f in files_exists:
                r.append(f)

        return r

    def get(self, urls, outs=None, bucket=None, flavors=("s3.small.1", "s3.medium.2")):

        if bucket is None:
            bucket = self.bucket
        if outs is None:
            outs = [u.split("/")[-1] for u in urls]

        size_all = 0
        for url in urls:
            size = self._get_content_size(url)
            if size == 0:
                e = "Can not get length of %r." % url
                LOG.error(e)
                raise Exception(e)
            else:
                size_all += size

        LOG.info("Download size: {:,}".format(size_all))
        disk_gb = self._get_disk_size_gb(size_all)
        uid = self._generate_id(urls)
        date = datetime.utcnow().strftime('%Y%m%d')
        folder = date + "/" + uid
        self.obs.mkdir(bucket, date+"/")

        files_exists = self._check_files_exists_in_obs(bucket, folder, outs)
        if len(files_exists) == len(outs):
            LOG.info("Download already.")
            return 0

        cfg = {
            "ak": self.ak,
            "sk": self.sk,
            "region": self.region,
            "bucket": self.bucket,
            "task": {
                uid: {
                    "date": date,
                    "urls": urls,
                    "outs": outs
                }
            },

        }

        zone = None
        flavor = None
        for f in flavors:
            zone = self.cloud.get_zone_has_flavor(f)
            if zone:
                flavor = f
                break

        if zone is None:
            e = "Flavors %s not found in region %s" % (flavors, self.region)
            LOG.error(e)
            raise Exception(e)

        server = self.cloud.create_service(
            name="download_%s" % uid,
            flavor=flavor,
            root_gb=disk_gb,
            image=self.image,
            personality={
                "path": "/etc/download.cfg",
                "content": json.dumps(cfg)
            },
            user_data="#! /bin/bash\npython -m hwget.server /etc/download.cfg\nshutdown -h now"
        )

        # wait for server shutdown

        curr_success = []
        while True:
            res = self.cloud.show_server(server)
            LOG.info("server %s is %s" % (server, res.status))
            files_exists = self._check_files_exists_in_obs(bucket, folder, outs)
            new_success = set(files_exists) - set(curr_success)
            if new_success:
                for n in new_success:
                    LOG.info("File %r downloaded." % n)

            curr_success = files_exists
            if res.status == "SHUTOFF":
                LOG.info("Delete server %s" % server)
                self.cloud.delete_server(server)
                failed = set(outs) - set(curr_success)
                if failed:
                    LOG.info("File %r failed." % failed)
                else:
                    LOG.info("All files downloaded.")
                break

            time.sleep(60)
