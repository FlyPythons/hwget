#-*- coding:utf-8 -*-
import os
from setuptools import setup, find_packages


def get_version():
    """Get version and version_info from markdown/__meta__.py file."""
    module_path = os.path.join(os.path.dirname('__file__'), 'hwget', 'version.py')

    import importlib.util
    spec = importlib.util.spec_from_file_location('version', module_path)
    meta = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(meta)
    return meta.__version__


setup(
    name="Hwget",
    author="Junpeng Fan",
    author_email="jpfan@whu.edu.cn",
    version=get_version(),
    packages=find_packages(),
    description='Download data with HuaWei cloud',
    url="https://github.com/FlyPythons/hwget",
)
