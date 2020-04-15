# -*- coding:utf-8 -*-
import os
import sys
from setuptools import setup, find_packages

PY_VERSION = 2
if sys.version[0] == "3":
    PY_VERSION = 3


def get_version():
    """Get version and version_info from markdown/__meta__.py file."""
    module_path = os.path.join(os.path.dirname('__file__'), 'hwget', 'version.py')

    meta = {}
    with open(module_path) as fh:
        exec(fh.read(), meta)
        return meta["__version__"]


def get_requirements():
    r = []
    for line in open("requirements.txt"):
        line = line.strip()
        if line:
            r.append(line)

    return r


setup(
    name="Hwget",
    author="Junpeng Fan",
    author_email="jpfan@whu.edu.cn",
    version=get_version(),
    packages=find_packages(),
    install_requires=get_requirements(),
    description='Download data with HuaWei cloud',
    url="https://github.com/FlyPythons/hwget",
)

