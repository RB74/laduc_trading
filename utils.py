import os
import errno
import json
import pytz
import time
import yagmail
import configparser
import numpy as np
from pandas import Timestamp
import pandas_market_calendars as mcal
from Laduc_SQL import SQLClient as RSQL
from datetime import datetime, timedelta
from Laduc_WordPress import WordPressClientInit as WPInit
import cachetools

DEV_MODE = True
# Set to None to automatically determine DEV_MODE (default True if on local machine).


if DEV_MODE is None:
    DEV_MODE = os.path.exists("C:/Users/zbarge")
elif not os.path.exists("C:/Users/zbarge") and DEV_MODE:
    DEV_MODE = False

print("DEV_MODE: {}".format(DEV_MODE))

CACHE_10_SEC = cachetools.TTLCache(9999, 10)
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
LOG_DIR = DATA_DIR
GMAIL_CREDS_PATH = os.path.join(DATA_DIR, "gmail-creds-dev.json" if DEV_MODE else "gmail-creds.json")
CONFIG_PATH = os.path.join(DATA_DIR, 'config-dev.ini') \
    if DEV_MODE else os.path.join(os.path.dirname(__file__), 'config.ini')
config = configparser.ConfigParser()
with open(CONFIG_PATH, 'r') as fh:
    config.read_file(fh)
MONTH_ABV_TO_INT_MAP = {
    'JAN': '01',
    'FEB': '02',
    'MAR': '03',
    'APR': '04',
    'MAY': '05',
    'JUN': '06',
    'JUL': '07',
    'AUG': '08',
    'SEP': '09',
    'OCT': '10',
    'NOV': '11',
    'DEC': '12'
}

# Yagmail patch for Oauth
import yagmail.oauth2 as ya_oauth2
import yagmail.sender as ya_sender


def get_oauth2_info(oauth2_file):
    oauth2_file = os.path.expanduser(oauth2_file)
    if os.path.isfile(oauth2_file):
        with open(oauth2_file) as f:
            oauth2_info = json.load(f)
        if not oauth2_info.get('google_refresh_token', None):
            refresh_token, _, _ = ya_oauth2.get_authorization(
                oauth2_info['google_client_id'],
                oauth2_info['google_client_secret'])
            oauth2_info['google_refresh_token'] = refresh_token.strip()
            with open(oauth2_file, "w") as f:
                json.dump(oauth2_info, f)
    else:
        print("If you do not have an app registered for your email sending purposes, visit:")
        print("https://console.developers.google.com")
        print("and create a new project.\n")
        email_addr = ya_oauth2.getpass.getpass("Your 'email address': ")
        google_client_id = ya_oauth2.getpass.getpass("Your 'google_client_id': ")
        google_client_secret = ya_oauth2.getpass.getpass("Your 'google_client_secret': ")
        google_refresh_token, _, _ = ya_oauth2.get_authorization(google_client_id, google_client_secret)
        oauth2_info = {"email_address": email_addr,
                       "google_client_id": google_client_id.strip(),
                       "google_client_secret": google_client_secret.strip(),
                       "google_refresh_token": google_refresh_token.strip()}
        with open(oauth2_file, "w") as f:
            json.dump(oauth2_info, f)
    return oauth2_info


ya_oauth2.get_oauth2_info = get_oauth2_info
ya_sender.get_oauth2_info = get_oauth2_info


def send_notification(subject, contents, to=None):
    if to is None:
        to = config['default']['notification_email']
    yagmail.SMTP(oauth2_file=GMAIL_CREDS_PATH).send(
        to=to, subject=subject, contents=contents)


def get_uid():
    return str(int(time.time()*1000))


def replace_sheet_formula_cells(value_list, old_row, new_row):
    """
    Replaces the old row index with the new row index in DataEntry gsheet formulas.
    :param value_list: (iterable) A row from the DataEntry sheet.
    :param old_row: (int, str) The current row of the value_list.
    :param new_row: (int, str) The new row of the value_list.
    :return: (iterable) The value_list updated in-place.
    """
    old_row, new_row = str(old_row), str(new_row)
    for i in range(14, 21):
        try:
            value_list[i] = value_list[i].replace(old_row, new_row)
        except (TypeError, AttributeError):
            continue
    return value_list

_NUM_CHARS = ['.', '-']


def try_float(x):
    v = ''.join(e for e in str(x) if e in _NUM_CHARS or e.isdigit()).strip()
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def main():
    """
    The main method to execute all the utility operations   
    Parameters : None
    
    Return values: None
    """
    # Updating the categories in the database
    update_cats()


def to_timestamp(x):
    if not x:
        return None
    try:
        return Timestamp(x)
    except:
        return None


def ensure_price(x):
    x = ''.join(e for e in str(x) if e.isdigit() or e in (',', '.', '-'))
    try:
        return float(x.split(',')[0])
    except ValueError:
        return None


def get_prices_list(x, count=3, default=None):
    prices = [ensure_price(y) or default for y in str(x).split(',')]
    if len(prices) < count:
        prices.extend([default for _ in range(count - len(prices))])
    return prices[:count]


def get_parsed_bag_tactic(t, symbol):
    t = str(t).upper().strip()
    legs_raw = t.split('/' if '/' in t else ',')
    legs_parsed = []
    for leg in legs_raw:
        try:
            action, md, year, pt, qty = leg.strip().split(' ')
            cur_month = None
        except:
            action, md, pt, qty = leg.strip().split(' ')
            _now = datetime.utcnow()
            cur_month = _now.strftime('%m')
            while cur_month.startswith('0'):
                cur_month = cur_month[1:]
            cur_month = int(cur_month)
            year = _now.strftime('%Y')

        strike = float(''.join(e for e in pt if e.isdigit() or e == '.'))
        side = 'C' if pt.endswith('C') else 'P'
        day = ''.join(e for e in md if e.isdigit())
        month = md.replace(str(day), '')
        month = MONTH_ABV_TO_INT_MAP[month]
        if cur_month and cur_month > int(month):
            year = str(int(year) + 1)

        legs_parsed.append(
            {
                'action': 'BUY' if action == 'BOT' else 'SELL',
                'year': year,
                'day': ensure_two_digit_int_str(day),
                'month': month,
                'strike': strike,
                'side': side,
                'symbol': symbol,
                'qty': int(qty.replace('X', '').strip())
            })

    return legs_parsed


def get_parsed_option_tactic(t, self):
    if t.endswith('C'):
        self.side = 'C'
    elif t.endswith('P'):
        self.side = 'P'

    parts = t.split(' ')
    day = ''.join(e for e in parts[0] if e.isdigit())

    if day:
        # Month/day are attached.
        month_abv = parts[0].replace(day, '').strip()[:3]
        self.expiry_month = MONTH_ABV_TO_INT_MAP[month_abv]
        self.expiry_day = int(day)
        if len(parts) >= 3:
            # Assume the 2nd part is the year
            self.expiry_year = int(parts[1])
            # Leaving the 3rd part for the strike
            strike_part = parts[2]
        else:
            # No expiry year
            strike_part = parts[1]
    else:
        # Month/day are separated
        self.expiry_month = MONTH_ABV_TO_INT_MAP[parts[0][:3]]
        self.expiry_day = parts[1]
        if len(parts) >= 4:
            self.expiry_year = int(parts[2])
            strike_part = parts[3]
        else:
            # No expiry year
            strike_part = parts[2]

    self.strike = float(''.join(e for e in strike_part if e.isdigit() or e == '.'))
    self.expiry_day = ensure_two_digit_int_str(self.expiry_day)


def get_closest_value_from_list(value, options):
    closest = min(options, key=lambda x: abs(x - value))
    for idx, opt in enumerate(options):
        if opt == closest:
            return idx, closest


def ensure_int_from_pct(x):
    return int_or_0(ensure_price(x))


def ensure_two_digit_int_str(x: int):
    x = str(x)
    if len(x) == 1:
        x = '0' + x
    while len(x) > 2:
        x = x[:-1]
    return x


def update_cats():
    """
    Updating all the category slugs for corresponding category ids  
    Parameters : None
    
    Return values: None
    """
    # It is initializing WordPress 
    WP = WPInit()
    # It is list of all categories from WordPress
    categories = WP.get_categories()
    if categories:
        rows = []
        
        # It is iterating over all categories
        for row in categories:
            obj = {}
            obj['id'] = row['id']
            obj['slug'] = row['slug'].strip()
            # It is appending 'id' and 'slug ' object into rows
            rows.append(obj)
        
        try:
            # It is initializing SQL
            SQL = RSQL()
            # it is setting response (True/False) after pushing data into 'wp_cat_slugs' Sql table
            resp = SQL.wp_update_categories(rows)
            print(resp)
        except Exception as e:
            # If exception occurs then it is printing exception
            print('update_cats -', repr(e))
        finally:
            try:
                SQL.close()
            except:
                pass


def int_or_0(i):
    try:
        return int(i)
    except:
        return 0


def track_json(file, data):
    if os.path.exists(file):
        # Prevents settings loss
        o_data = read_json(file)
        o_data.update(data)
        data = o_data
    for _ in range(0, 5):
        try:
            with open(file, 'w') as fh:
                json.dump(data, fh, indent=2, sort_keys=True)
                return
        except OSError as e:
            if e.errno == errno.ENOENT:
                raise
            continue


def read_json(file):
    if not os.path.exists(file):
        return dict()
    for _ in range(0, 5):
        try:
            with open(file, 'r') as fh:
                return json.load(fh)
        except OSError as e:
            if e.errno == errno.ENOENT:
                raise
            continue


def now_string():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def now_est():
    return utc_to_est(datetime.utcnow())


def utc_to_est(x):
    t = pytz.timezone('UTC').localize(x)
    return t.astimezone(pytz.timezone('US/Eastern'))


def est_to_utc(x):
    x = to_timestamp(x)
    if x is None:
        return x
    t = pytz.timezone('US/Eastern').localize(x)
    return t.astimezone(pytz.timezone('UTC')).replace(tzinfo=None)


def get_hours_to_market_open():
    return get_seconds_to_market_open()/60/60


def get_minutes_to_market_open():
    return get_seconds_to_market_open()/60


def get_seconds_to_market_open():
    try:
        return CACHE_10_SEC['seconds_to_market']
    except KeyError:
        pass

    est = pytz.timezone('US/Eastern')
    now = datetime.now(est)
    nyse = mcal.get_calendar('NYSE')
    mkt_open = nyse.schedule(now, now + timedelta(days=5))
    next_open = mkt_open.iloc[0]['market_open'].astimezone(est)
    CACHE_10_SEC['seconds_to_market'] = res = (next_open - now).total_seconds()

    return res


def now_is_rth():
    return get_seconds_to_market_open() < 0


def is_localtime_old(local_time, old_seconds=60):
    return local_time < datetime.now() - timedelta(seconds=old_seconds)



if __name__ == '__main__':
    #Calling main method
    #main()
    print(get_uid())
    t = pytz.timezone('UTC').localize(datetime.utcnow())
    t.astimezone(pytz.timezone('US/Eastern')).strftime('%m/%d/%Y %H:%M')

