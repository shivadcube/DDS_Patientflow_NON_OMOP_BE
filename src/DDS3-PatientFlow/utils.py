import dateutil
import functools
import hashlib
import os
import re
import time
import uuid

from datetime import date, datetime, timedelta
from datetime import time as datetime_time
from sys import getsizeof

from django.conf import settings
from django.contrib.auth import login
from django.contrib.auth.models import User
from django.middleware import csrf
from django.db.models.query_utils import Q
from django.template import Context
from django.template import Template
from django.utils import timezone
from django.utils.timezone import now

import pytz
from boto.s3.connection import S3Connection
from boto.s3.key import Key

WEEKDAYS_FULL = ['Monday', 'Tuesday', 'Wednesday', 'Thursday',
                 'Friday', 'Saturday', 'Sunday']
WEEKDAYS_SHORT = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']


def get_weekday_full_string(weekday):
    return WEEKDAYS_FULL[weekday]


def get_weekday_short_string(weekday):
    return WEEKDAYS_SHORT[weekday]


def get_image_path(instance, filename):
    return os.path.join('photos', str(instance.id), filename)


def get_start_of_day():
    _today = today()
    result = datetime(_today.year, _today.month, _today.day, tzinfo=pytz.UTC)
    return result


def today(now_func=None):
    """Returns today's date as per users local timezone."""
    if not now_func:
        now_func = timezone.now
    time_now_in_utc = now_func()
    time_now_in_local = timezone.localtime(time_now_in_utc)
    return time_now_in_local.date()


def today_date_utc():
    return timezone.now().date()


def yesterday_date_utc():
    return get_x_days_date_before_today(1)


def get_x_days_date_before_today(days):
    return today_date_UTC() - timedelta(days=days)


def convert_utc_to_local_time(time=None):
    if not time:
        return
    time_date_in_local = timezone.localtime(time)
    return time_date_in_local


localtime = timezone.localtime


def get_timestamp(datetime_obj=None):
    """ Returns the current timestamp (epoch in seconds).
    """
    datetime_obj = datetime_obj or datetime.utcnow()
    return time.mktime((datetime_obj).timetuple())


def convert_timestamp_to_datetime(timestamp):
    """Returns datetime in UTC equivalent to the given timestamp.

    Args:
      timestamp: A float representing the timestamp (epoch in seconds).
    """
    return datetime.utcfromtimestamp(timestamp).replace(tzinfo=pytz.UTC)


def get_datetime_from_datestr(datestr, format):
    return datetime.fromtimestamp(time.mktime(time.strptime(datestr, format)))


def get_date_from_datestr(datestr, format='%d/%m/%Y'):
    if not datestr:
        return
    return get_datetime_from_datestr(datestr, format).date()


def get_datestr_from_date(date, format='%d/%b/%Y'):
    return date.strftime(format)


def get_datetimestr_from_datetime(datetime, format='%H:%M %d/%m/%Y'):
    return datetime.strftime(format)


def convert_date_utc(date_str):
    if not date_str:
        return
    date_formats = [
        '%a, %b %d %Y',
        '%a %b %d %Y',
        '%a %d %b,%Y',
        '%a, %b %d, %Y'
    ]
    for format in date_formats:
        try:
            return datetime.strptime(date_str, format).date()
        except ValueError:
            continue


def get_seconds_since_midnight(date=None):
    if date and date != today():
        return 24 * 3600
    local_time = datetime.now()
    return local_time.hour * 3600 + local_time.minute * 60 + local_time.second


def get_timezones_with_offest():
    result = []
    for tz in get_timezones():
        result.append('%s %s' % (tz, now(pytz.timezone(tz)).strftime('%z')))
    return result


def get_timezones():
    return ['Asia/Kolkata', 'Asia/Singapore', 'US/Alaska', 'US/Arizona',
            'US/Central', 'US/Eastern', 'US/Hawaii', 'US/Mountain',
            'US/Pacific', 'Australia/Melbourne', 'Australia/Sydney',
            'Europe/London']


def get_diff_in_weeks(d1, d2):
    monday1 = (d1 - timedelta(days=d1.weekday()))
    monday2 = (d2 - timedelta(days=d2.weekday()))
    return (monday2 - monday1).days / 7


def convert_from_rangex_to_rangey(old_value, old_lower, old_upper,
                                  new_lower, new_upper):
    old_range = (old_upper - old_lower)
    new_range = (new_upper - new_lower)
    new_value = (((old_value - old_lower) * new_range) / old_range) + new_lower
    return new_value


def is_in_range(value, val1, val2):
    if val1 <= value <= val2 or val2 <= value <= val1:
        return True
    return False


def print_starred_text(text):
    print('*' * 30 + '  ' + text)


def print_line(length=87):
    print('-' * length)


def get_client_ip(request):
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        ip = x_forwarded_for.split(',')[0]
    else:
        ip = request.META.get('REMOTE_ADDR')
    return ip


def get_today_in_datetime():
    today = timezone.now()
    return today.replace(hour=0, minute=0, second=0)


def did_user_join_before_x_days(user, x):
    today = get_today_in_datetime()
    return user.date_joined < today - timedelta(x)


def all_subclasses(cls):
    subclasses = cls.__subclasses__() + [
        g for s in cls.__subclasses__() for g in all_subclasses(s)]
    return subclasses


def memoize(fn):
    @functools.wraps(fn)
    def memoizer(context, *args):
        key = fn.__name__ + str(args)
        if key in context:
            return context[key]
        else:
            value = fn(context, *args)
            context[key] = value
            return value

    return memoizer


def list_to_text(array):
    text = ""
    if len(array) == 0:
        return text
    count = 0
    if len(array) == 1:
        return array[0]
    while count < len(array) - 1:
        text += array[count]
        text += ", "
        count += 1
    text = text[:-2]
    text += " and %s" % array[len(array) - 1]
    return text


def _get_random_hex():
    return str(uuid.uuid1().hex)


def datetime_to_iso_format(datetime_obj):
    return datetime_obj.strftime("%Y%m%dT%H%M%S%Z")


def iso_to_datetime_format(iso_str):
    return dateutil.parser.parse(iso_str)


# very simple check
def is_valid_email(email):
    if re.match("^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$", email):
        return True
    return False


def x_is_what_percent_of_y(x, y):
    try:
        return (float(x) / y) * 100
    except ZeroDivisionError:
        return 0


def is_url_an_image(url):
    response = urllib2.urlopen(url)
    content_type = response.info().getheader('Content-Type')
    if content_type in ['image/jpeg', 'image/png', 'image/gif']:
        return True
    return False


def get_user_from_username_or_email(username_or_email):
    """Returns User object that matches given usename or email.

    throws:
       User.DoesNotExist if no user matches.
    """
    return User.objects.get(
        Q(username=username_or_email) | Q(email=username_or_email))


def get_closest_higher_value(input_value, value_array):
    """Returns a number that is closest ceil of the input value in the value_array
    """
    for value in value_array:
        if value > input_value:
            return value
    return -1


class CatchExceptions(object):
    def __init__(self, exceptions=None, errorreturn=None, errorcall=None):
        if exceptions is None:
            exceptions = [Exception]
        self.exceptions = exceptions
        self.errorreturn = errorreturn
        self.errorcall = errorcall

    def __call__(self, function):
        def returnfunction(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            except Exception as e:
                ignore_exception = False
                for exception in self.exceptions:
                    if isinstance(e, exception):
                        ignore_exception = True
                if not ignore_exception:
                    raise e
                if self.errorcall is not None:
                    self.errorcall(e, *args, **kwargs)
                return self.errorreturn

        return returnfunction


def log_exception(*args, **kwargs):
    print('args(%s) kwargs(%s)' % (args, kwargs))


def get_datetime_with_min_time(min_datetime):
    min_datetime = datetime.combine(min_datetime.date(),
                                    datetime_time.min)
    min_datetime = timezone.make_aware(min_datetime,
                                       timezone.get_current_timezone())
    return min_datetime


def get_datetime_with_max_time(max_datetime):
    max_datetime = datetime.combine(max_datetime.date(),
                                    datetime_time.max)
    max_datetime = timezone.make_aware(max_datetime,
                                       timezone.get_current_timezone())
    return max_datetime


def make_tz_aware(datetime_obj):
    return timezone.make_aware(datetime_obj,
                               timezone.get_current_timezone())


def validate_international_phone_number(value):
    rule = re.compile(r'(^\+{0,1}[1-9]{0,1}[0-9]{10,11}$)')
    if value.startswith('+91'):
        rule = re.compile(r'(^\+91[0-9]{10}$)')
    if rule.search(value):
        return True
    return False


def remove_special_chars_from_phone_number(value):
    """value should be a string
    """
    if not value:
        return ''
    return ''.join(i for i in value if i.isdigit())


def validate_email_domain(username, domain):
    domain = domain.lower()
    username = username.lower()
    return bool(re.search(r'[\w]+\@(\w*\.)?' + domain, username))


def validate_email_domain_in(username, domains):
    for domain in domains:
        if validate_email_domain(username, domain):
            return True
    return False


def get_email_domain(email_id):
    email_split = email_id.split('@')
    if len(email_split) == 2 and len(email_split[1]) > 1:
        domain_name = email_split[1]
        domain_name_splits = domain_name.split('.')
        return '.'.join(domain_name_splits[-2:])


def get_week_monday(date):
    date = date or timezone.datetime.today().date()
    return date - timezone.timedelta(date.weekday())


def get_week_boundary(date):
    start_date = get_week_monday(date)
    end_date = start_date + timezone.timedelta(days=6)
    return start_date, end_date


def get_user_agent_string(request):
    if not (hasattr(request, 'META') and request.META.get('HTTP_USER_AGENT')):
        return ''
    return request.META.get('HTTP_USER_AGENT').lower()


def get_date_string(date):
    if not date:
        return
    if date.day == 1:
        return '1st'
    elif date.day == 2:
        return '2nd'
    elif date.day == 3:
        return '3rd'
    else:
        return str(date.day) + 'th'


def get_min_start_max_end_date(start_date=None, end_date=None):
    if not start_date:
        start_date = timezone.now()
    if not end_date:
        end_date = timezone.now()
    tz = pytz.timezone('Asia/Kolkata')
    start_time = tz.localize(datetime.combine(start_date, datetime.min.time()))
    end_time = tz.localize(datetime.combine(end_date, datetime.max.time()))
    return start_time, end_time


def get_min_start_max_end_datetime_iso_format(start_date=None, end_date=None):
    start_time, end_time = get_min_start_max_end_date(start_date=start_date,
                                                      end_date=end_date)
    return start_time.isoformat(), end_time.isoformat()


def get_min_start_max_end_date_iso_format(start_date=None, end_date=None):
    start_time, end_time = get_min_start_max_end_date(start_date=start_date,
                                                      end_date=end_date)
    return start_time.date().isoformat(), end_time.date().isoformat()


def settings_environment(request):
    """ Adds a template context variable containing the environment.

    The variable can be accessed in any template using {{ environ }}
    Possible values are local, staging, and prod
    """
    return {'environ': getattr(settings, 'ENVIRON', 'unknown')}


def split_list(the_list, chunk_size):
    if chunk_size == 0:
        return [the_list]
    result_list = []
    while the_list:
        result_list.append(the_list[:chunk_size])
        the_list = the_list[chunk_size:]
    return result_list


def get_static_file_path(base_string, username):
    return settings.STATIC_ROOT + base_string + '_' + str(
        get_timestamp()) + '_' + username


def upload_competetion_image_to_aws(competetion, user, submission, image):
    aws_s3_base_url = "https://s3-ap-southeast-1.amazonaws.com/%s/" % settings.AWS_COMPETETION_BUCKET
    conn = S3Connection(settings.AWS_S3_ACCESS_ID, settings.AWS_S3_ACCESS_KEY)
    bucket = conn.get_bucket(settings.AWS_COMPETETION_BUCKET)
    identifier = str(submission.id) + str(user.username)
    hex_hash = hashlib.md5(
        os.urandom(16).encode('hex') + hashlib.md5(identifier).hexdigest()
    ).hexdigest()
    k = Key(bucket)
    k.key = str(competetion.id) + '/' + hex_hash
    k.set_metadata('submissionid', str(submission.id))
    k.set_metadata('S3uploadtime', str(now()))
    k.set_metadata('Content-Type', 'image/png')
    k.set_contents_from_string(image.read())
    k.set_acl('public-read')
    url = aws_s3_base_url + str(competetion.id) + '/' + hex_hash
    return url
