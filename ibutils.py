import math
import pytz
import utils
import ibapi
import logging
from tzlocal import get_localzone
from datetime import datetime, timedelta
import ibapi.connection as ib_connection
from ibapi.contract import Contract as IBContract, ComboLeg
log = logging.getLogger(__name__)
LOCAL_TZ = get_localzone()

def clean_ib_package():
    import pkgutil

    class FakeLock(object):
        def acquire(self): pass

        def release(self): pass

    class NoLockConnection(ib_connection.Connection):
        def __init__(self, *args, **kwargs):
            super(NoLockConnection, self).__init__(*args, **kwargs)
            setattr(self, 'lock', FakeLock())

    # Drop IB logging.
    for _, module_name, _ in pkgutil.iter_modules(ibapi.__path__):
        try:
            __import__("ibapi." + module_name, fromlist=["ibapi"]).logger.setLevel(logging.CRITICAL)
        except AttributeError:
            pass

    # Swap IB locking with a dummy object.
    ib_connection.Connection = NoLockConnection
    ibapi.client.Connection = NoLockConnection


clean_ib_package()


def get_stock_contract(symbol, exchange='SMART'):
    c = Contract()
    c.symbol = symbol
    c.secType = 'STK'
    c.exchange = exchange
    if exchange == 'SMART':
        c.primaryExchange = 'ISLAND'
    c.currency = 'USD'
    return c


def get_cash_contract(symbol, exchange='IDEALPRO'):
    c = Contract()
    c.symbol = symbol
    c.secType = 'CASH'
    c.exchange = exchange
    c.currency = 'USD'
    return c


def get_utc_from_server_time(t):
    dt = datetime.strptime(t, '%Y%m%d %H:%M:%S')
    dt2 = LOCAL_TZ.localize(dt)
    return dt2.astimezone(pytz.timezone('UTC'))


def get_option_contract_from_contract_key(contract_id):
    try:
        symbol, expiration, strike, right = contract_id.split("-")
        return get_option_contract(symbol, strike, expiration, right)
    except:
        return None

def get_option_contract(symbol, strike, expiration, type, exchange='SMART'):
    """
    :param symbol:
    :param strike: (int, float) The strike price.
    :param expiration: (str) The option expiration date
        march 15 2019 would be expressed as "20190315"
    :param type: (str) 'C' for a call, 'P' for a put.
    :param exchange:

    :return:
    """
    c = Contract()
    c.symbol = symbol
    c.secType = "OPT"
    c.exchange = exchange
    c.currency = "USD"
    c.lastTradeDateOrContractMonth = expiration
    c.strike = float(strike)
    c.right = type
    c.multiplier = "100"
    return c


def get_ratio(qty1, qty2):
    if qty1 == qty2:
        return [1, 1]

    gcd = math.gcd(qty1, qty2)
    if gcd > 0:
        return [qty1/gcd, qty2/gcd]
    return [qty1, qty2]


def get_ratio2(qtys):
    same_qtys = True
    qtys2 = qtys.copy()
    for qty in qtys2:
        for qty2 in qtys:
            if qty != qty2:
                same_qtys = False

    if same_qtys:
        ratios = [1 for _ in range(len(qtys))]
    else:
        ratios = list()
        for qty in qtys:
            ratio = 1
            for qty2 in qtys2:
                cd = qty/qty2
                if cd > 1:
                    ratio = cd
            ratios.append(int(ratio))
    return ratios


def get_bag_contract(legs, ib_app=None):
    c = Contract()
    c.symbol = ','.join(list(set([leg['symbol'] for leg in legs])))
    c.secType = 'BAG'
    c.currency = 'USD'
    c.exchange = 'SMART'
    c.comboLegs = []

    # Set ratios
    qtys = [leg['qty'] for leg in legs]
    ratios = get_ratio2(qtys)
    for ratio, leg in zip(ratios, legs):
        leg['ratio'] = ratio

    # Setup comboLegs and attach to contract.
    # They'll be attached to the order via IbApp.place_order()
    for leg in legs:
        leg['exchange'] = leg.get('exchange', 'SMART')
        c_leg = ComboLeg()
        c_leg.action = leg['action']
        c_leg.exchange = leg['exchange']
        c_leg.ratio = leg['ratio']
        c.comboLegs.append(c_leg)

        expiration = '{}{}{}'.format(leg['year'], leg['month'], leg['day'])
        leg['expiration'] = expiration
        leg_contract = get_option_contract(
            leg['symbol'], leg['strike'],
            expiration, leg['side'],
            leg.get('exchange', c.exchange)
        )
        leg['contract'] = leg_contract
        leg['combo_leg'] = c_leg
        c_leg.__parent_contract = c
        if ib_app is not None:
            ib_app.request_leg_id(leg_contract, c_leg)
            leg['_requested'] = True
        else:
            leg['_requested'] = False
        log.debug("Combo leg: action='{action}, exchange='{exchange}',"
                  "ratio='{ratio}',expiration='{expiration}',"
                  "symbol='{symbol}', strike='{strike}',"
                  "side='{side}'".format(**leg))
    return c


def send_closing_trade_notification(trade, price, force=False):
    subject = '{}: DE Sheet Update Needed'.format(trade.symbol)
    contents = "Symbol: {}\nUID: {}\nEntry Underlying Price: {}\nEntry Price: {}\nExit Price: {}\nClosing Reason: {}".format(
        trade.symbol, trade.u_id, trade.underlying_entry_price, trade.entry_price, price, trade.close_reason
    )
    if not force and trade.sheet.init_time < datetime.now() - timedelta(minutes=3):
        log.info(contents)
    else:
        utils.send_notification(subject, contents, utils.config['ib']['notification_email'])


class Contract(IBContract):
    @property
    def key(self):
        """
        :return (str) a unique Contract Key.
        """
        if self.secType == 'OPT':
            return "{}-{}-{}-{}".format(
                self.symbol,
                self.lastTradeDateOrContractMonth,
                self.strike, self.right)
        elif self.secType == 'BAG':
            id = '-'.join(['{}/{}'.format(c.action, c.ratio) for c in self.comboLegs])
            return '{}/{}/{}'.format(self.symbol, self.secType, id)
        else:
            return self.symbol

    @staticmethod
    def from_ib(ib_contract):
        c = Contract()
        [setattr(c, k, v) for k, v in ib_contract.__dict__.items() if hasattr(c, k)]
        if not c.exchange:
            c.exchange = 'SMART'
        return c

    def get_price_key(self, tick_type):
        return '{}_{}'.format(self.secType, tick_type).lower()

    def get_time_key(self, tick_type):
        return '{}_time'.format(self.get_price_key(tick_type))
