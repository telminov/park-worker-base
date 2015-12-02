# coding: utf-8
# python setup.py sdist register upload
from setuptools import setup

setup(
    name='park-worker-base',
    version='0.1.2',
    description='Base logic of workers for park-keeper project.',
    author='Telminov Sergey',
    url='https://github.com/telminov/park-worker-base',
    packages=[
        'parkworker',
        'parkworker/asyncio',
    ],
    license='The MIT License',
    # test_suite='parkworker.tests',
    test_suite='runtests.runtests',
    install_requires=[
        'pytz', 'pyzmq', 'mongoengine==0.10.0'
    ],
    tests_require=[
        'django-park-keeper', 'factory_boy',
    ]
)
