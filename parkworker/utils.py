# coding: utf-8
import datetime
import os

import sys

import pytz


def now():
    return datetime.datetime.utcnow().replace(tzinfo=pytz.utc)

