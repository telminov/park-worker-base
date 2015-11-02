# coding: utf-8
import datetime
import pytz


def now():
    return datetime.datetime.utcnow().replace(tzinfo=pytz.utc)


def json_default(obj):
    """Default JSON serializer."""
    import calendar, datetime

    if isinstance(obj, datetime.datetime):
        if obj.utcoffset() is not None:
            obj = obj - obj.utcoffset()
    millis = int(
        calendar.timegm(obj.timetuple()) * 1000 +
        obj.microsecond / 1000
    )
    return millis
