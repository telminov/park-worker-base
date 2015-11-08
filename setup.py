# coding: utf-8
# python setup.py sdist register upload
from distutils.core import setup

setup(
    name='park-worker-base',
    version='0.0.5',
    description='Base logic of workers for park-keeper project.',
    author='Telminov Sergey',
    url='https://github.com/telminov/park-worker-base',
    packages=[
        'parkworker',
        'parkworker/monits',
    ],
    license='The MIT License',
    install_requires=[
        'pytz',
    ],
)
