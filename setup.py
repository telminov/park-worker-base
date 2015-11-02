# coding: utf-8
from distutils.core import setup

setup(
    name='park-worker-base',
    version='0.0.1',
    description='Base logic of workers for park-keeper project.',
    author='Telminov Sergey',
    url='https://github.com/telminov/park-worker-base',
    packages=['parkworker',],
    license='The MIT License',
    install_requires=[
        'pytz',
    ],
)
