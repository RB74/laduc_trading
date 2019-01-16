import os
import pytz
import math
import utils
import ibapi
import gspread
import logging
from time import sleep
from threading import Thread
from collections import defaultdict
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from ibapi import wrapper
from ibapi.order import Order
from ibapi.client import EClient
from ibapi.utils import iswrapper
from ibapi.ticktype import TickTypeEnum
import ibapi.connection as ib_connection
from ibapi.execution import Execution, ExecutionFilter
from ibapi.contract import Contract as IBContract, ComboLeg

PRODUCTION_SHEET_ID = '1p8rr5tmroFuKNyko40jYJmK7PwEGIVHCkPxlW446LIk'
TEST_SHEET_ID = '1aBxtmUXH2miPi8DigvPEz9kg6kRuTXzhkD61gV9ZLC4'


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
config = utils.config
LOG_PATH = os.path.join(utils.LOG_DIR, 'ib.log')
SHEET_TEST_MODE = config['ib'].getboolean('sheet_test_mode', True)
CLOSE_OPEN_ON_START = config['ib'].getboolean('close_open_positions_on_start', False)

if SHEET_TEST_MODE:
    settings_filename = 'ib_cfg_z.json'
    GSHEET_ID = TEST_SHEET_ID
else:
    settings_filename = 'ib_cfg.json'
    GSHEET_ID = PRODUCTION_SHEET_ID

SETTINGS_FILE = os.path.join(utils.DATA_DIR, 'ib_cfg.json' )
logging.basicConfig(level=logging.DEBUG, filename=LOG_PATH, format='%(asctime)s %(message)s')
log = logging.getLogger(__name__)

tick_type_map = {
    TickTypeEnum.BID: 'bid',
    TickTypeEnum.ASK: 'ask',
    TickTypeEnum.CLOSE: 'close'
}
STOP_LOSS = 'Stop loss'
TGT_REACHED = 'Target reached'


def get_stock_contract(symbol, exchange='SMART'):
    c = Contract()
    c.symbol = symbol
    c.secType = 'STK'
    c.exchange = exchange
    if exchange == 'SMART':
        c.primaryExchange = 'ISLAND'
    c.currency = 'USD'
    return c


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


def get_bag_contract(legs, ib_app):
    c = Contract()
    c.symbol = ','.join(list(set([legs[0]['symbol'], legs[1]['symbol']])))
    c.secType = 'BAG'
    c.currency = 'USD'
    c.exchange = 'SMART'
    c.comboLegs = []
    legs[0]['ratio'], legs[1]['ratio'] = get_ratio(int(legs[0]['qty']), int(legs[1]['qty']))

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
        c_leg.__parent_contract = c
        ib_app.request_leg_id(leg_contract, c_leg)
        log.debug("Combo leg: action='{action}, exchange='{exchange}',"
                  "ratio='{ratio}',expiration='{expiration}',"
                  "symbol='{symbol}', strike='{strike}',"
                  "side='{side}'".format(**leg))
    return c


def get_data_entry_sheet():
    for i in range(3):
        try:
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                os.path.join(os.path.dirname(__file__), 'service-credentials.json'),
                ['https://spreadsheets.google.com/feeds'])
            return gspread.authorize(credentials)\
                .open_by_key(GSHEET_ID)\
                .worksheet('DataEntry')
        except gspread.exceptions.APIError:
            if i == 2:
                raise


def get_data_entry_rows():
    return get_data_entry_sheet().get_all_values()[1:]


def get_data_entry_trades(trade_sheet=None, rows=None):
    if not rows:
        rows = get_data_entry_rows()

    return [
        Trade.from_gsheet_row(row, trade_sheet, row_idx=idx)
        for idx, row in enumerate(rows, start=2)
        if row            # Yes Row w/ data
        and row[0]        # Yes Symbol
        and row[11]       # Yes date entered
        and not row[12]   # No date exited
    ]


def send_closing_trade_notification(trade, price, force=False):
    subject = '{}: DE Sheet Update Needed'.format(trade.symbol)
    contents = "Symbol: {}\nUID: {}\nEntry Underlying Price: {}\nEntry Price: {}\nExit Price: {}\nClosing Reason: {}".format(
        trade.symbol, trade.u_id, trade.underlying_entry_price, trade.entry_price, price, trade.close_reason
    )
    if not force and trade.sheet.init_time < datetime.now() - timedelta(minutes=3):
        log.info(contents)
    else:
        utils.send_notification(subject, contents, config['ib']['notification_email'])


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
                self.strike,
                self.right)
        elif self.secType == 'BAG':
            return '{}-{}-{}'.format(
                self.symbol,
                '-'.join(['{} @ {}'.format(c.action, c.ratio) for c in self.comboLegs]),
                self.secType
            )
        else:
            return self.symbol

    @staticmethod
    def from_ib(ib_contract):
        c = Contract()
        [setattr(c, k, v) for k, v in ib_contract.__dict__.items() if hasattr(c, k)]
        if not c.exchange:
            c.exchange = 'SMART'
        return c


class Wrapper(wrapper.EWrapper):
    pass


class Client(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)


class IbApp(Wrapper, Client):
    def __init__(self,
                 settings=None,
                 trade_sheet=None,
                 test=False,
                 subscribe=True,
                 eval_freq=60*1,
                 execute_trades=None):
        Wrapper.__init__(self)
        Client.__init__(self, wrapper=self)
        settings = settings if settings is not None else utils.read_json(SETTINGS_FILE)
        if execute_trades is None:
            execute_trades = config['ib'].getboolean('execute_trades', False)

        self.settings = settings
        self.trade_sheet = trade_sheet if trade_sheet is not None else TradeSheet(self)
        self.test = test if test else settings.get('test_mode', test)
        self.subscribe = subscribe
        self.eval_freq = eval_freq
        self.execute_trades = execute_trades

        self.nextValidOrderId = None
        self.account_id = settings['account_id_test'] if self.test else settings['account_id_production']
        self.balances = defaultdict(dict)
        self.portfolio = defaultdict(dict)
        self.prices = defaultdict(dict)

        self._acc_download_first_cycle = True
        self._connected = False
        self._contracts_by_req = dict()
        self._reqs_by_contract_key = dict()
        self._eval_time = None
        self._combo_legs = dict()
        self._contract_ids = dict()
        self._contract_keys_by_req = dict()
        self._contract_success = dict()
        self._trades_w_order = dict()
        self.executions = defaultdict(list)

    def connect(self, host='127.0.0.1', port=4001, _id=1):
        if port in [4001, 4002]:
            port = 4002 if self.test else 4001
        _id = 2 if _id == 1 and not self.subscribe else _id
        super().connect(host, port, _id)

    @staticmethod
    def next_id():
        data = utils.read_json(SETTINGS_FILE)
        last_id = data.get('last_id', 0)
        last_id += 1
        utils.track_json(SETTINGS_FILE, {'last_id': last_id})
        return last_id

    @iswrapper
    def nextValidId(self, orderId: int):
        self.nextValidOrderId = orderId
        if self.subscribe:
            self.reqAccountUpdates(True, self.account_id)
            self.reqAllOpenOrders()
            self.trade_sheet.sync_trades()

    @iswrapper
    def contractDetails(self, req_id, details):
        try:
            leg = self._combo_legs.pop(req_id)
            leg.conId = details.underConId
            log.debug("Updated comboLeg for {}".format(details.underSymbol))
            contract_key = self._contract_keys_by_req[req_id]
            self._contract_ids[contract_key] = details.underConId

            # Check parent's legs..if they all have a conId then reqMktData.
            parent = getattr(leg, '__parent_contract', None)
            if parent is None:
                log.error("__parent_contract should've been assigned to option leg but wasn't.")
            else:
                self._register_contract(parent)
        except KeyError:
            pass

    def request_executions(self, filter=False):
        if self._trades_w_order and filter:
            min_time = min(self._trades_w_order.values(),
                           key=lambda x: x.closing_order_time_placed).closing_order_time_placed
            exec_filter = ExecutionFilter()
            # TODO: Fix error here. If Needed?
            exec_filter.time = min_time.strftime('%Y%m%d %H:%M')
        else:
            exec_filter = ExecutionFilter()
        self.reqExecutions(self.next_id(), exec_filter)

    def request_leg_id(self, contract, combo_leg):
        try:
            combo_leg.conId = self._contract_ids[contract.key]
        except KeyError:
            pass

        req_id = self.next_id()
        self._contract_keys_by_req[req_id] = contract.key
        self._combo_legs[req_id] = combo_leg
        self.reqContractDetails(req_id, contract)

    def register_trade(self, trade):
        """
        Registers a new Trade with IB.
        Possibly requests pricing data for Stock and/or options.

        :param trade: (Trade) The Trade to be registered.
        :return: (None)
        """
        contract = trade.get_contract()
        try:
            return self._reqs_by_contract_key[contract.key]
        except KeyError:
            pass
        except AttributeError:
            log.error("Error getting contract for {}".format(trade))
            return

        self._register_contract(contract)

        # Maybe also register Stock Contract so we can evaluate stop_loss/target_price(s)
        if contract.secType in ('OPT', 'BAG'):
            stk_contract = trade.get_stock_contract()
            if stk_contract.key not in self._reqs_by_contract_key:
                self._register_contract(stk_contract)

    def _register_contract(self, contract):
        if contract.secType == 'BAG':
            # BAG contracts need to wait for leg contractIDs.
            for leg in contract.comboLegs:
                if not leg.conId:
                    return False

        req_id = self.next_id()
        self._reqs_by_contract_key[contract.key] = req_id
        self._contracts_by_req[req_id] = contract
        self._contract_success[contract.key] = False
        self.reqMktData(req_id, contract, "", False, False, [])
        log.debug("request({}): {} market data for contract: "
                  "{}: {}".format(req_id, contract.secType, contract.key, contract))

        return req_id

    def sync_market_data_subscriptions(self):
        if not utils.now_is_rth():
            return log.debug("Skipping market data subscription sync outside of RTH.")

        check_time = datetime.now() - timedelta(seconds=60)
        contract_dates = self._contract_success.copy()
        for contract_key, last_seen in contract_dates.items():
            matches = self.trade_sheet.get_trades_by_contract_key(contract_key)
            if not matches:
                continue
            if not last_seen:
                oldest_date = min(matches, key=lambda t: t.date_entered).date_entered
                if self.trade_sheet.init_time < datetime.now() - timedelta(minutes=30)\
                        and oldest_date > datetime.now() - timedelta(minutes=8):
                    # Never successfully seen price on this contract.
                    for trade in matches:
                        if trade.key not in self.trade_sheet.invalid_trades:
                            # highlight entry price.
                            trade.highlight_cell(9, bg_color='red')
                            self.trade_sheet.invalid_trades[trade.key] = trade

            elif last_seen and last_seen < check_time:
                # Last price was seen too long ago.
                self.cancelMktData(self._reqs_by_contract_key.pop(contract_key))
                self._contract_success.pop(contract_key)
                log.debug("Cancelled market data for {}".format(contract_key))

    def place_order(self, contract=None, order=None, qty=None, action=None, trade=None):
        """
        Places an market order to IB.

        :param contract:
        :param order:
        :param qty:
        :param action: (str, default 'BUY') options = ('BUY', 'SELL', "SSHORT')
        :return:
        """
        if trade is not None:
            if trade.locked:
                return log.error("Cannot place an order on an already locked trade: {}".format(trade))
            if not trade.entry_price:
                if qty is None:
                    qty = trade.get_open_size()
                if action is None:
                    action = trade.get_open_action()
            else:
                if action is None:
                    action = 'BUY' if trade.get_open_action() == 'SELL' else 'SELL'

        if qty is None:
            qty = 1
        if action is None:
            action = 'BUY'

        if contract is None:
            contract = trade.get_contract()

        order_id = self.next_id()

        if order is None:
            order = Order()
            order.action = action
            order.orderType = 'MKT'
            order.totalQuantity = qty

        log.info("api.place_order({}) >> {} {} "
                 "{}".format(order_id, order.action, order.totalQuantity, contract.key))

        super().placeOrder(order_id, contract, order)

        if trade is not None:
            trade.orders[order_id] = order
            self._trades_w_order[order_id] = trade
            trade.closing_order_time_placed = datetime.now()
            trade.lock()

        return order_id

    @iswrapper
    def accountSummary(self, req_id, account, tag, value, currency):
        """
        self.accounts = {'acc1': {'Currency': None, 'CashBalance': None,
                  'TotalCashBalance': None, 'AccruedCash': None,
                  'UnrealizedPnL': None, 'RealizedPnL': None,
                  'ExchangeRate': None, 'FundValue': None,
                  'NetDividend': None}}
        """
        super().accountSummary(req_id, account, tag, value, currency)
        log.info("{}: message for account: {} --> {}: "
                 "{}".format(datetime.now(), account, tag, value))
        self.balances[account][tag] = value
        data = {'type': 'account_summary',
                'key': tag,
                'value': value,
                'time': utils.now_string()
                }

    @iswrapper
    def updateAccountValue(self, key: str, val: str,
                           currency: str, account_name: str):
        super().updateAccountValue(key, val, currency, account_name)
        self.balances[account_name][key] = val
        data = {'type': 'account_value',
                'key': key,
                'value': val,
                'account_name': account_name,
                'time': utils.now_string()
                }

    def get_price_data_by_symbol(self, symbol, sec_type_key=None):
        reqs = [k for k, v in self._contracts_by_req.items()
                if v.symbol == symbol
                and (not sec_type_key
                     or sec_type_key[0] != v.secType
                     or (sec_type_key[0] == v.secType
                         and sec_type_key[1] == v.key))]
        data = dict()
        for req_id in reqs:
            data.update(self.prices[req_id])
        return data

    def get_midpoint_by_symbol(self, symbol, validate=True, prices=None, sec_type='stk', contract_key=None):
        if not prices:
            if sec_type != 'stk' and contract_key is not None:
                sec_type_key = (sec_type, contract_key)
            else:
                sec_type_key = None
            prices = self.get_price_data_by_symbol(symbol, sec_type_key=sec_type_key)
            if not prices:
                return None

        bid_key = '{}_bid'.format(sec_type)
        ask_key = '{}_ask'.format(sec_type)

        bid = prices.get(bid_key, None)
        ask = prices.get(ask_key, None)
        if not all((bid, ask)):
            return None

        if validate:
            bid_time_key = bid_key + '_time'
            ask_time_key = ask_key + '_time'
            bid_time = prices.get(bid_time_key, None)
            ask_time = prices.get(ask_time_key, None)
            if not all((bid_time, ask_time)):
                return None
            if utils.is_localtime_old(bid_time, old_seconds=30):
                return None
            if utils.is_localtime_old(ask_time, old_seconds=30):
                return None

        return round((bid + ask) / 2, 2)

    @iswrapper
    def tickPrice(self, req_id, tick_type, price, attrib):
        if price < 0 or attrib.preOpen:
            return

        # Store price
        contract = self._contracts_by_req[req_id]
        tick_type = tick_type_map.get(tick_type, tick_type)
        key1 = '{}_{}'.format(contract.secType, tick_type).lower()
        self.prices[req_id][key1] = float(price)
        self.prices[req_id][key1 + '_time'] = datetime.now()
        #log.debug("{}: {} @ {}".format(contract.symbol, key1, price))
        # Track contract success.
        try:
            contract = self._contracts_by_req[req_id]
            self._contract_success[contract.key] = datetime.now()
        except KeyError:
            log.error("Failed to track contract "
                      "success for req_id {}".format(req_id))

        # Print StdOut
        if contract.secType == 'OPT':
            print("{} / {} {}: {} {}: {} {}".format(
                utils.now_string(),
                contract.symbol,
                contract.lastTradeDateOrContractMonth,
                contract.strike,
                contract.right, tick_type, price))
        else:
            print("{} / {}: {} {}".format(utils.now_string(), contract.symbol, tick_type, price))

        # Execute evaluation logic on interval.
        self.evaluate_trades()

    @iswrapper
    def tickOptionComputation(self, tickerId, field, impliedVolatility, delta,
                              optPrice, pvDividend, gamma, vega, theta, undPrice):
        # Store price data
        p = self.prices[tickerId]
        p['opt_implied_volatility'] = impliedVolatility
        p['opt_delta'] = delta
        p['opt_price'] = optPrice
        p['opt_dividend'] = pvDividend
        p['opt_gamma'] = gamma
        p['opt_vega'] = vega
        p['opt_theta'] = theta
        p['opt_und_price'] = undPrice

        # Track contract success
        try:
            contract = self._contracts_by_req[tickerId]
            self._contract_success[contract.key] = datetime.now()
        except KeyError:
            log.error("Failed to track contract "
                      "success for tickerId {}".format(tickerId))

    def evaluate_trades(self):
        """
        Calls the IBApp.evaluate_trade method on each open trade as long
        as the appropriate time interval has passed since the last time this method was called.
        The eval_freq kwarg is used to determine the appropriate time interval.
        :return:
        """
        eval_time = self._eval_time if self._eval_time else self.trade_sheet.init_time
        if eval_time > datetime.now() - timedelta(seconds=self.eval_freq):
            # Don't evaluate trades until interval has passed.
            return

        # On eval_freq interval:
        elif self._trades_w_order and self._eval_time:
            # Try to keep trades_w_order clear.
            self.request_executions()

        log.debug("Evaluating trades.")
        self._eval_time = datetime.now()

        # Evaluate unlocked trades.
        trades = [t for t in self.trade_sheet.trades.values() if not t.locked]
        for trade in trades:
            self.evaluate_trade(trade)

        # Sync market data subscriptions
        self.sync_market_data_subscriptions()

    def evaluate_trade(self, trade):
        """
        Evaluates a Trade object to see if it should be closed. Closes the trade when a
        target price or stop price has been reached.

        :param trade: (Trade) The Trade object to be evaluated.
        :return: None
        """

        # Get Stock bid/ask to evaluate stop/targets.
        sc = trade.get_stock_contract()
        prices = self.get_price_data_by_symbol(
            trade.symbol,
            sec_type_key=None if trade.sec_type in ('CASH', 'STK') \
                    else (trade.sec_type, trade.get_contract().key)
        )
        if prices is None:
            return False

        try:
            req_id = self._reqs_by_contract_key[sc.key]
            p = self.prices[req_id]
            stk_bid = prices['stk_bid']
            stk_ask = prices['stk_ask']
        except KeyError:
            log.error("IbApp.evaluate_trade() error getting bid/ask: {}".format(trade))
            return False

        if not trade.entry_price:
            if not trade.date_entered or trade.date_entered < datetime.now() - timedelta(days=5):
                trade.valid = False
                log.error("Ignoring trade for old/missing date_entered: {}".format(trade))
                return False
            else:
                self.place_order(qty=trade.get_open_size(),
                                 action=trade.get_open_action(),
                                 trade=trade)
                return False

        price = (stk_ask + stk_bid) / 2
        partial_exit_no = len(trade.partial_exits) or 1
        target_price = getattr(trade, "target_price{}".format(partial_exit_no))
        stop_price = getattr(trade, "stop_price{}".format(partial_exit_no))

        target_reached = (trade.profits_up and price >= target_price)\
                      or (trade.profits_down and price <= target_price)

        if not stop_price:
            stop_reached = False
        else:
            stop_reached = (trade.profits_up and price <= trade.stop_price)\
                        or (trade.profits_down and price >= trade.stop_price)

        if target_reached or stop_reached:
            if target_reached:
                close_reason = TGT_REACHED
                close_size = abs(trade.get_target_size())
            else:
                close_reason = STOP_LOSS
                close_size = abs(trade.get_stop_size())

            trade.close_reason = close_reason
            if trade.sec_type in ('OPT', 'BAG'):
                oc = trade.get_contract()
                try:
                    #req_id = self._reqs_by_contract_key[oc.key]
                    #p = self.prices[req_id]
                    opt_bid = prices['opt_bid']
                    opt_ask = prices['opt_ask']
                except KeyError:
                    log.error("IbApp.evaluate_trade() error getting opt_bid/opt_ask: "
                              "{}\nprices data: {}".format(trade, p))
                    return None
                mkt_price = opt_bid if trade.size_open > 0 else opt_ask
                price = round(mkt_price, 2) if self.execute_trades else (opt_bid + opt_ask) / 2
            elif self.execute_trades:
                mkt_price = stk_bid if trade.size_open > 0 else stk_ask
                price = round(mkt_price, 2)

            self.close_trade(trade, price, close_size)

    def get_checked_order_qty(self, trade, qty, action):
        if action == 'SELL':
            # Can we actually sell?
            # Find the portfolio for an open trade
            acc_key = list(self.portfolio.keys())[0]
            portfolio = self.portfolio[acc_key]
            trade_contract = trade.get_contract()
            c_key = trade_contract.key
            try:
                acc_data = portfolio[acc_key]
            except KeyError:
                # No reference to this in portfolio...
                # We cannot sell a position we dont have.
                return 0
            cur_qty = acc_data['position']
            if cur_qty >= qty:
                return qty
            elif cur_qty < 0:
                return 0
        return qty



    def close_trade(self, trade, price, qty, timestamp='now', execution=None):
        """
        Closes a given Trade either partially or fully.

        :param trade: (Trade) The trade to be closed.
        :param price: (float) The price to close the trade at.
        :param timestamp (DateTime, str) The time to show the trade is closed.
        :return: None
        """
        log.debug("Trade action triggered: {}".format(trade))
        size_open = trade.size_open
        if abs(qty) > abs(size_open):
            raise ValueError("Trade only has {} available yet close_trade qty is {}.".format(size_open, qty))

        if execution is None and self.execute_trades:

            # Expect a call back later with an execution once complete.
            action = 'BUY' if size_open < 0 else 'SELL'
            qty = self.get_checked_order_qty(trade, qty, action)
            if qty == 0:
                trade.valid = False
                trade.lock()
                log.debug("Locking invalid trade {} for "
                          "failed order qty check".format(trade))
                return None
            self.place_order(
                qty=qty,
                action=action,
                trade=trade)
        else:
            if timestamp == 'now':
                timestamp = utils.now_est()

            trade.close(price, timestamp)
            self.trade_sheet.close_trade(trade)
            self.trade_sheet.sync_trades(force=True)

    @iswrapper
    def updatePortfolio(self, contract: Contract, position: float,
                        market_price: float, market_value: float,
                        avg_cost: float, unrealized_pnl: float,
                        realized_pnl: float, account_name: str):
        super().updatePortfolio(contract, position, market_price, market_value,
                                avg_cost, unrealized_pnl, realized_pnl, account_name)
        contract = Contract.from_ib(contract)
        data = {
            'symbol': contract.symbol,
            'security_type': contract.secType,
            'position': float(position),
            'market_price': float(market_price),
            'account_name': account_name,
            'time': datetime.now(),
            'contract': contract
        }
        self.portfolio[account_name][contract.key] = data
        log.debug(data)

    @iswrapper
    def accountDownloadEnd(self, account_name):

        if not utils.now_is_rth():
            return log.debug("Skipping GSheet/IB sync (outside Regular Trading Hours).")

        portfolio = self.portfolio[account_name]
        log.debug("Evaluating portfolio ({} positions).".format(len(portfolio)))

        # Maybe close all open positions
        if self._acc_download_first_cycle:
            self._acc_download_first_cycle = False
            if CLOSE_OPEN_ON_START:
                for contract_key, data in portfolio.items():
                    if data['position'] == 0:
                        continue
                    contract = data['contract']
                    action = 'BUY' if data['position'] < 0 else 'SELL'
                    self.place_order(contract, qty=abs(data['position']), action=action)
                portfolio.clear()

        sync_open = False
        if sync_open:
            # Sync open positions w/ GSheet
            for contract_key, data in portfolio.items():
                trades = self.trade_sheet.get_trades_by_contract_key(contract_key)
                position = data['position']
                total_open = sum([trade.size_open for trade in trades])

                if 0 < total_open > position:
                    # Need increased long position
                    # e.g. 10 total_open in gsheet, 5 position in IB = 5 long needed.
                    diff = total_open - position
                    contract = trades[0].get_contract()
                    log.debug("{}: Adding long by {} to {}.".format(contract.key, diff, total_open))
                    self.place_order(contract, qty=diff, action='BUY')
                elif 0 > total_open < position:
                    # Need increased short position
                    # e.g -15 short on gsheet, -10 short on IB = -5 short needed.
                    diff = abs(total_open - position)
                    contract = trades[0].get_contract()
                    self.place_order(contract, qty=diff, action='SSHORT')
                    log.debug("{}: Adding short by {} to {}.".format(contract.key, diff, total_open))

                # TODO: Handle position decreases?

        # Sync new GSheet positions w/ IB
        trades = defaultdict(list)
        for trade in self.trade_sheet.trades.values():
            c = trade.get_contract()
            if c is not None and c.key not in portfolio:
                trades[c.key].append(trade)

        for contract_key, trade_list in trades.items():
            symbol = trade_list[0].symbol
            mid = self.get_midpoint_by_symbol(symbol)
            if not mid:
                continue

            for trade in trade_list:
                logical, reason = trade.get_whether_logical_to_open(mid)
                if not logical:
                    trade.valid = False
                    trade.lock()
                    log.debug("Locking invalid trade ({}) {}".format(reason, trade))
                    continue
                else:
                    self.place_order(trade=trade)

    @iswrapper
    def execDetails(self, req_id: int, contract: Contract, execution: Execution):
        contract = Contract.from_ib(contract)
        self.executions[req_id].append((contract, execution))
        data = {
                'type': 'execution',
                'order_id': execution.execId,
                'symbol': contract.symbol,
                'quantity': execution.shares,
                'action': execution.side,
                'price': execution.price,
                'insert_date': execution.time,
                'time': datetime.now()
        }

        matches = self.trade_sheet.get_trades_by_contract_key(contract.key)
        for trade in matches:
            if not trade.locked or execution.orderId not in trade.orders:
                continue

            if not trade.entry_price:
                # Newly entered trade - Update GSheet with exec price.
                log.debug("Execution for trade open received: {}".format(trade))
                trade.register_entry_price(execution.price)
                self._trades_w_order.pop(execution.orderId)

            elif not trade.exit_price:
                # Completed trade: update GSheet/close out.
                order = trade.orders[execution.orderId]
                if execution.cumQty < order.totalQuantity:
                    # Wait for execution to meet order qty.
                    continue
                log.debug("Execution for trade close received: {}".format(trade))
                self.close_trade(trade, execution.avgPrice, execution.cumQty, execution=execution)
                #trade.close(execution.avgPrice, utils.now_est())
                self._trades_w_order.pop(execution.orderId)
            else:
                continue
            trade.unlock()
        log.debug("Execution order id ({order_id}) - "
                  "{action} {quantity} {symbol} @ {price}".format(**data))

    @iswrapper
    def execDetailsEnd(self, req_id: int):
        # We're going to use the latest market price available.
        log.debug("Execution details end for request: {}".format(req_id))
        self.trade_sheet.sync_trades(force=True)

    @iswrapper
    def error(self, error_id, error_code, error_msg):
        if 'farm connection is OK' in error_msg:
            return
        if 'farm is connecting' in error_msg:
            return
        data = {'type': 'error',
                'error_code': error_code,
                'error_msg': error_msg,
                'time': utils.now_string()}
        msg = "API ERROR: {time}: ({error_code}) {error_msg}".format(**data)
        log.error(msg)
        print(msg)


class IbAppThreaded(Thread):
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.app = None
        Thread.__init__(self)

    def run(self):
        self.app = IbApp(**self.kwargs)
        self.app.connect()
        self.app.run()


class Trade:
    """
    Represents a trade entered into the DataEntry sheet.
    Interacts with the DataEntry GSheet & Interactive Brokers.
    """
    __slots__ = [
        'symbol', 'date_entered', 'date_exited', 'entry_price',
        'underlying_entry_price', 'target_price', 'stop_price', 'u_id', 'tactic',
        'sec_type', 'side', 'expiry_month', 'expiry_day', 'expiry_year', 'strike', 'sheet',
        'close_reason', 'valid', 'exchange', 'leg_data', 'alert_category', 'size',
        '_opt_contract', '_stk_contract', '_cash_contract', '_bag_contract',
        'closing_order_time_placed', 'target_price1', 'target_price2', 'target_price3',
        'stop_price1', 'stop_price2', 'partial_exits', 'pct_sold', 'exit_price', 'row_idx',
        'orders', '__locked'
    ]

    def __init__(self, **kwargs):

        # DataEntry row values
        self.symbol = None
        self.alert_category = None
        self.size = None
        self.date_entered = None
        self.date_exited = None
        self.entry_price = None
        self.exit_price = None
        self.underlying_entry_price = None
        self.u_id = None
        self.row_idx = None
        self.tactic = None

        self.stop_price = None
        self.stop_price1 = None
        self.stop_price2 = None

        self.target_price = None
        self.target_price1 = None
        self.target_price2 = None
        self.target_price3 = None
        self.orders = dict()
        self.partial_exits = list()
        self.pct_sold = None

        # Parsed from tactic
        self.sec_type = None        # str (STK/OPT/CASH/BAG)
        self.side = None            # str (C/P)
        self.expiry_month = None    # int
        self.expiry_day = None      # int
        self.expiry_year = None     # int
        self.strike = None          # float
        self.leg_data = None        # list (contains above data for multi-leg)

        self.close_reason = None
        self.valid = False
        self.exchange = 'SMART'
        self.closing_order_time_placed = None
        self.sheet = None

        # Contract Types
        self._opt_contract = None
        self._stk_contract = None
        self._cash_contract = None
        self._bag_contract = None

        self.__locked = False

        [setattr(self, k, v) for k, v in kwargs.items() if k in self.__slots__]

    @property
    def closing_side(self):
        return 'BUY' if self.is_short else 'SELL'

    @property
    def is_long(self):
        return self.size > 0

    @property
    def is_short(self):
        return self.size < 0

    @property
    def opening_side(self):
        return self.get_open_action()

    @property
    def key(self):
        try:
            ts = self.date_entered.strftime('%Y%m%d%H%M')
        except AttributeError:
            ts = ''
        return '{}-{}-{}-{}'.format(
            self.symbol,
            ts,
            self.underlying_entry_price,
            self.target_price)

    @property
    def locked(self):
        return self.__locked

    @property
    def profits_down(self):
        # Standardized way to tell if a trade is a short or long
        # regardless of security type. We trust the data-enterer.
        return self.target_price1 < self.underlying_entry_price

    @property
    def profits_up(self):
        return not self.profits_down

    @property
    def size_open(self):
        size = self._calc_portion_size(1)
        for trade in self.partial_exits:
            pct_sold = trade.pct_sold/100
            amt_closed = size * pct_sold
            size -= amt_closed

        return round(size, 0)

    @property
    def stopped(self):
        return self.close_reason == STOP_LOSS

    @property
    def target_reached(self):
        return self.close_reason == TGT_REACHED

    def get_target_size(self):
        return self._calc_portion_size(self.number_of_targets)

    def get_stop_size(self):
        return self._calc_portion_size(self.number_of_stops)

    def get_open_size(self):
        return self._calc_portion_size(1)

    def get_open_action(self):
        if self.sec_type == 'BAG':
            # TODO: When would we sell/short? a bag?
            return 'BUY'
        return 'SSHORT' if self.size < 0 else 'BUY'

    def get_whether_logical_to_open(self, current_underlying):
        logical = True
        reasons = []
        if self.locked:
            logical = False
            reasons.append("Trade is locked.")

        if self.sec_type in ('STK', 'OPT', 'CASH'):
            if self.stop_price1:
                if (self.profits_up and current_underlying < self.stop_price1)\
                      or (self.profits_down and current_underlying > self.stop_price1):
                    logical = False
                    reasons.append('Stop price ({}) has hit based on underlying ' \
                             '{}'.format(self.stop_price1, current_underlying))
            if self.target_price1:
                if (self.profits_up and current_underlying > self.target_price1)\
                        or (self.profits_down and current_underlying < self.target_price1):
                    logical = False
                    reasons.append('Target price ({}) has hit based on underlying ' \
                             '{}'.format(self.stop_price1, current_underlying))
        if self.date_entered:
            logical = False
            reasons.append("Pre-existing DATE ENTERED.")
        if self.date_exited:
            logical = False
            reasons.append("Pre-existing DATE EXITED.")
        log.error("Determined not logical to open Trade for reason(s): "
                  "{}, {}".format(reasons, self))

        return logical, reasons

    def lock(self):
        if self.__locked:
            raise Exception("Already locked: {}".format(self))
        self.__locked = True

    def unlock(self):
        if not self.__locked:
            raise Exception("Already unlocked: {}".format(self))
        self.__locked = False

    @property
    def pct_left(self):
        pct = 100
        for trade in self.partial_exits:
            if trade.pct_sold is not None:
                pct -= trade.pct_sold
        return pct

    @property
    def number_of_targets(self):
        return self._add_qtys(self.target_price1, self.target_price2, self.target_price3)

    @property
    def number_of_stops(self):
        return self._add_qtys(self.stop_price1, self.stop_price2)

    @staticmethod
    def from_gsheet_row(row, trade_sheet=None, row_idx=None):
        """
        Returns a new Trade assigning variables
        by positional index of GSheet columns.

        :param row: (iterable) A row of data indexed like the DataEntry GSheet.
        :return: (Trade) A Trade object with variables assigned.
        """
        t = Trade()
        t.alert_category = row[0]
        t.symbol = row[1]
        t.size = utils.ensure_price((row[2]))
        t.tactic = row[3]
        t.underlying_entry_price = utils.ensure_price(row[5])
        t.stop_price = utils.ensure_price(row[6])
        t.stop_price1, t.stop_price2 = utils.get_prices_list(row[6], count=2)
        t.target_price = utils.ensure_price(row[7])
        t.target_price1, t.target_price2, t.target_price3 = utils.get_prices_list(row[7], count=3)
        t.entry_price = utils.ensure_price(row[8])
        t.pct_sold = utils.ensure_int_from_pct(row[9])
        t.exit_price = utils.ensure_price(row[10])
        t.date_entered = utils.to_timestamp(row[11])
        t.date_exited = utils.to_timestamp(row[12])
        t.u_id = row[21]
        t.sheet = trade_sheet
        t.row_idx = row_idx
        t.parse_tactic()
        t._determine_direction()

        # null stop = manual trade mgmt.
        if not t.stop_price:
            t.stop_price = None

        return t

    def close(self, price, timestamp, validate=True, email_only=False):
        """
        Records a partial of complete exit of the Trade
        on the GSheet or via e-mail.

        :param price: (int, float, str)
            The price to close the trade at.
        :param timestamp: (str, object<strftime>)
            A time-like object ideally expressed (or expressable via strftime) as
                MM/DD/YYYY HH:MM
            This format is not enforced when this parameter is a string.
        :param validate: (bool, default True)
            True will run the trade through a series of True/False validations.
            If false is returned by Trade._validate_close_data(price, timestamp)
            the trade will not be updated.
        :param email_only (bool, default False)
            True would force the close to send an email notification rather than update
            the GSheet (testing purposes)
        :return: (bool)
            True = success, False = failure.
        """
        # Maybe check validity of Trade execution.
        if validate:
            close_data = self._validate_close_data(price, timestamp)
            if not close_data:
                log.debug("Invalid close_data for trade (prevents close): "
                          "{}".format(self))
                return False
            price, timestamp = close_data

        # Determine if partial close
        is_partial_sale, close_pct = self.get_partial_close_decision()

        if not email_only or config['ib'].getboolean('test_mode') is False:
            # Update GSheet
            if is_partial_sale:
                # Insert Trade row (copy): record partial exit.
                self.close_partial(price, timestamp, close_pct)
            else:
                # Update Trade row: record complete exit.
                log.debug("Closing trade: {}".format(self))
                sheet = get_data_entry_sheet()
                row = sheet.findall(self.u_id)[0].row
                # 1 index = 1 (not 0 like Python)
                sheet.update_cell(row, 10, '{}%'.format(close_pct))  # % Sold
                sheet.update_cell(row, 11, '$' + str(price))         # Exit Price
                sheet.update_cell(row, 13, timestamp)                # Date Exited
                sheet.update_cell(row, 14, self.close_reason)        # Notes
                sheet.update_cell(row, 22, utils.get_uid())          # UID
        else:
            # Send a notification email.
            log.debug("Sending trade close notification: {}".format(self))
            send_closing_trade_notification(self, price)

        return True

    def close_partial(self, price, timestamp, pct, validate=True):
        """
        Partially closes a trade in the GSheet by copy/inserting a new
        row with % Sold, Exit Price, Date Exited, Notes, UID updated.

        :param price: (int, float, str)
            The price to close the trade at.
        :param timestamp: (str, object<strftime>)
            A time-like object ideally expressed (or expressable via strftime) as
                MM/DD/YYYY HH:MM
            This format is not enforced when this parameter is a string.
        :param pct: (int, float, str)
            An integer-like percentage of the trade to show as closed.
            This value should be between 0 and 100 and will not be multiplied/divided.
        :param validate: (bool, default True)
            True will run the trade through a series of True/False validations.
            If false is returned by Trade._validate_close_data(price, timestamp)
            the trade will not be updated.
        :return:
        """

        log.debug("Closing partial: {}".format(self))
        if validate:
            close_data = self._validate_close_data(price, timestamp)
            if not close_data:
                return False
            price, timestamp = close_data

        sheet = get_data_entry_sheet()
        og_row = sheet.findall(self.u_id)[0].row
        n_row = og_row+1

        # Specifically grab formula expressions from GSheet.
        values = sheet.row_values(og_row, value_render_option='FORMULA')
        values = values + ['' for _ in range(22-len(values))]

        # Replaces the old row # with the new one in formula cells.
        utils.replace_sheet_formula_cells(values, og_row, n_row)

        values[9] = '{}%'.format(pct)   # % Sold
        values[10] = '$' + str(price)   # Exit Price
        values[12] = timestamp          # Date Exited
        values[13] = self.close_reason  # Notes
        values[21] = utils.get_uid()    # UID

        # Insert the new row.
        sheet.insert_row(values, index=n_row, value_input_option='USER_ENTERED')

        # Register partial trade
        trade = Trade.from_gsheet_row([str(v) for v in values], trade_sheet=self.sheet)
        self.partial_exits.append(trade)

    def get_partial_close_decision(self):
        """
        Determines whether or not the trade should be closed as a partial sale or closed entirely.
        :return: (tuple[bool, float])
            bool: True = yes partial sale, False = no partial sale.
            float: The closing percentage as an integer (between 0 - 100)
        """
        if self.stopped and self.number_of_stops > len(self.partial_exits) > 1:
            is_partial_sale = True
            close_pct = round(100/self.number_of_stops, 0)
        elif self.target_reached and self.number_of_targets > len(self.partial_exits) > 1:
            is_partial_sale = True
            close_pct = round(100/self.number_of_targets, 0)
        else:
            is_partial_sale = False
            close_pct = self.pct_left
        return is_partial_sale, close_pct

    def get_contract(self):
        if self.sec_type == 'STK':
            return self.get_stock_contract()
        elif self.sec_type == 'OPT':
            return self.get_option_contract()
        elif self.sec_type == 'CASH':
            return self.get_cash_contract()
        elif self.sec_type == 'BAG':
            return self.get_bag_contract()

    def get_bag_contract(self):
        if not self.valid or not self.leg_data:
            return None
        elif self._bag_contract is not None:
            return self._bag_contract

        self._bag_contract = get_bag_contract(self.leg_data, self.sheet.app)
        return self._bag_contract

    def get_cash_contract(self):
        if self._cash_contract is None and self.sec_type == 'CASH':
            c = get_stock_contract(self.symbol, self.exchange)
            c.secType = self.sec_type
            self._cash_contract = c
        return self._cash_contract

    def get_option_contract(self):
        if not self.valid:
            return None
        elif self._opt_contract is not None:
            return self._opt_contract

        now_ts = datetime.now()

        # TODO: Handle multi-leg contracts
        expiration = '{}{}{}'.format(
            self.expiry_year or now_ts.strftime('%Y'),
            self.expiry_month,
            self.expiry_day
        )

        if datetime.strptime(expiration, '%Y%m%d') < now_ts:
            expiration = '{}{}{}'.format(
                (now_ts + timedelta(days=365)).strftime('%Y'),
                self.expiry_month,
                self.expiry_day
            )

        self._opt_contract = get_option_contract(
            self.symbol, self.strike, expiration,
            self.side, exchange=self.exchange
        )
        return self._opt_contract

    def get_stock_contract(self):
        if self._stk_contract is None:
            self._stk_contract = get_stock_contract(self.symbol, exchange=self.exchange)
        return self._stk_contract

    def parse_tactic(self):
        try:
            self._parse_tactic()
            self.valid = True
        except (IndexError, ValueError, KeyError, TypeError) as e:
            if self.u_id not in self.sheet.invalid_trades:
                log.debug("Error parsing tactic: {} >> {}".format(self.tactic, e))
                self.sheet.invalid_trades[self.u_id] = self


    def highlight_cell(self, col_number, bg_color='red'):
        if not self.u_id and not self.row_idx:
            return log.debug("Cannot highlight cell (missing u_id/row_idx) - {}".format(self))

        from gspread_formatting import CellFormat, format_cell_range, Color
        from gspread.utils import rowcol_to_a1
        color_map = {'red': Color(1),
                     'white': Color(1, 1, 1)}
        color = color_map[bg_color]
        sheet = get_data_entry_sheet()
        fmt = CellFormat(backgroundColor=color)
        if self.u_id:
            row = sheet.findall(self.u_id)[0].row
        else:
            row = self.row_idx
        format_cell_range(sheet, rowcol_to_a1(row, col_number), fmt)

    def highlight_tactic(self, bg_color='red'):
        return self.highlight_cell(4, bg_color=bg_color)

    def init_new_trade(self):
        if self.u_id:
            return log.error("Cannot initialize trade from user that already has a u_id: {}".format(self))
        if self.date_entered and self.date_entered < datetime.now() - timedelta(days=2):
            return log.debug("Ignoring old unregistered trade: {}.".format(self))

        self.entry_price = None
        self.u_id = utils.get_uid()
        sheet = get_data_entry_sheet()
        # Remove entry_price, add u_id asap so we can find this row
        # later even if other rows are added/removed.

        sheet.update_cell(self.row_idx, 9, '')
        sheet.update_cell(self.row_idx, 22, self.u_id)

    def register_entry_price(self, price, overwrite=False):
        assert self.u_id, "Missing u_id on trade {}".format(self)
        if not overwrite:
            if self.entry_price:
                raise ValueError("Set overwrite=True to overwrite existing entry price.")

        sheet = get_data_entry_sheet()
        try:
            match = sheet.findall(self.u_id)[0]
        except IndexError:
            # uid was removed from the record.
            # Invalidate the trade.
            self.valid = False
            log.debug("UID was removed from trade - invalidating: {}".format(self))
            return
        sheet.update_cell(match.row, 9, price)
        self.highlight_cell(1, bg_color='white')
        self.highlight_cell(9, bg_color='white')

    def update_from_trade(self, trade):
        if self.locked or trade.locked:
            log.debug("Rejecting update on locked trade: {}".format(self))
            return False

        self.alert_category = trade.alert_category
        self.size = utils.ensure_price(trade.size)
        self.entry_price = trade.entry_price
        self.underlying_entry_price = trade.underlying_entry_price
        self.row_idx = trade.row_idx

        self.stop_price = trade.stop_price
        self.stop_price1 = trade.stop_price1
        self.stop_price2 = trade.stop_price2

        self.target_price = trade.target_price
        self.target_price1 = trade.target_price1
        self.target_price2 = trade.target_price2

        self.date_exited = trade.date_exited
        self.pct_sold = trade.pct_sold
        self.exit_price = trade.exit_price
        if trade.partial_exits:
            self.partial_exits = trade.partial_exits

    def _determine_direction(self):
        """
        Convert short positions internally.
        In GSheets: A negative entry price and exit price indicates a short.
        In IB:      A negative position size indicates short or long.
                    Entry/exit prices are positive floats.
        :return:
        """

        try:
            self.size = float(self.size)
        except (TypeError, ValueError):
            self.size = 1

        try:
            self.entry_price = float(self.entry_price)
        except (TypeError, ValueError):
            return  # Unknown.

        if not self.size:
            self.size = 1
        elif self.size < 0:
            return   # We know it's short.
        elif self.entry_price < 0:
            self.size = -abs(self.size)
            self.entry_price = abs(self.entry_price)
            return  # Converted to short.

    def _parse_tactic(self):
        t = self.tactic
        if not t:
            return
        t = str(t).upper().strip()

        # CASH
        if 'USD' in self.symbol and len(self.symbol) > 3:
            self.sec_type = 'CASH'
            self.exchange = 'IDEALPRO'
            self.symbol = self.symbol.replace('USD', '').strip()

        # STK
        elif 'STOCK' in t:
            self.sec_type = 'STK'
            if 'LONG' in t:
                self.side = 'C'
            elif 'SHORT' in t:
                self.side = 'P'
            else:
                raise AttributeError("tactic missing LONG/SHORT keyword.")

        # BAG
        # {ACTION} {MONTH}{DAY} {YEAR} {STRIKE}{SIDE} x{QTY} / {ACTION} {MONTH}{DAY} {YEAR} {STRIKE}{SIDE} x{QTY}
        elif ('/' in t or ',' in t) and 'X' in t:
            # Multi-leg contract
            # e.g: SLD 2018 DEC31 $100P x5/BOT 2019 JAN15 $100P x5
            self.sec_type = 'BAG'
            self.leg_data = utils.get_parsed_bag_tactic(t, self.symbol)

        # OPT
        # {MONTH}{DAY} {YEAR} {STRIKE}{SIDE} OR {MONTH}{DAY} {STRIKE}{SIDE}
        else:
            self.sec_type = 'OPT'
            utils.get_parsed_option_tactic(t, self)

    def _validate_close_data(self, price, timestamp):
        if not self.u_id or len(self.u_id) < 3:
            log.error("Failed to close trade (invalid UID) {}".format(self))
            return False
        elif self.date_exited:
            log.error("Failed to close trade (pre-existing date exited {}): {}".format(self.date_exited, self))
            return False
        elif self.exit_price:
            log.error("Failed to close trade (pre-existing exit price {}): {}".format(self))
        if not isinstance(timestamp, str):
            try:
                timestamp = timestamp.strftime('%m/%d/%Y %H:%M')
            except AttributeError:
                log.error("Failed to close trade (invalid timestamp {}): {}".format(timestamp, self))
                return False

        return price, timestamp

    def _calc_portion_size(self, number):
        if not self.entry_price:
            entry_price = self.sheet.app.get_midpoint_by_symbol(
                self.symbol, self.sec_type.lower())
            if entry_price is None:
                return self.size or 1
        else:
            entry_price = self.entry_price

        capital = self.size * 1000
        if self.sec_type in ('OPT', 'BAG'):
            price_per = entry_price*100
        else:
            price_per = entry_price
        quantity = round(capital / price_per, 0)
        return 1 if abs(quantity) < number else round(quantity / number, 0)

    def _add_qtys(self, *numbers):
        qty = 0
        for n in numbers:
            if n is not None:
                qty += 1
        return qty

    def __lt__(self, other):
        return self.date_entered < other.date_entered

    def __repr__(self):
        return "{}Trade(symbol='{}', entry_date='{}', und_entry='{}', " \
               "stop_price='{}', target='{}', u_id='{}', cont_id='{}')".format(
                str(self.sec_type).title(), self.symbol, self.date_entered, self.underlying_entry_price,
                self.stop_price, self.target_price, self.u_id,
                getattr(self.get_contract(), 'key', 'N/A')
        )


class TradeSheet:
    def __init__(self, ib_app=None, refresh_seconds=60*2):
        if ib_app is None:
            self._thread = IbAppThreaded(trade_sheet=self)
            ib_app = self._thread.app
        self.app = ib_app
        self._trades = dict()
        self._last_updated = None
        self.refresh_seconds = refresh_seconds
        self._delta = timedelta(seconds=refresh_seconds)
        self._closed = list()
        self._invalid_trades = dict()
        self.init_time = datetime.now()

    def get_trades_by_contract_key(self, key):
        return [t for t in self.trades.values() if getattr(t.get_contract(), 'key', '') == key]

    def get_trades_by_symbol(self, symbol):
        s = symbol.upper()
        return [t for t in sorted(self._trades.values()) if t.symbol.upper() == s]

    def close_trade(self, trade):
        # Remove trade/prevent from returning.
        # TODO: Rethink this for production.
        self._closed.append(trade.key)
        self._trades.pop(trade.key)

    @property
    def trades(self):
        self.sync_trades()
        return self._trades

    @property
    def invalid_trades(self):
        return self._invalid_trades

    def sync_trades(self, force=False):
        if (self._last_updated and self._last_updated <= datetime.now() - self._delta) \
                or not self._trades \
                or not self._last_updated\
                or force:
            log.debug("Syncing trades.")
        else:
            return self._trades

        self._last_updated = datetime.now()
        rows = get_data_entry_rows()
        current_trades = get_data_entry_trades(self, rows)
        log.debug("{} current trades".format(len(current_trades)))
        current_keys = [t.key for t in current_trades]

        # Add new current trades
        for t in current_trades:
            key = t.key

            if not t.u_id:
                # Remove entry price/add u_id
                t.init_new_trade()

            if not t.valid:
                if t.u_id not in self._invalid_trades:
                    # Highlight tactic on invalid trade.
                    log.debug("Invalid trade: {}".format(t))
                    self._invalid_trades[t.u_id] = t
                    t.highlight_tactic('red')
                # Skip invalid Trade
                continue

            try:
                log.debug("Updating existing trade: {}".format(key))
                existing_trade = self._trades[key]
                existing_trade.update_from_trade(t)
            except KeyError:

                if self.app is not None:
                    log.debug("Registering new trade with IB: {}".format(key))
                    self.app.register_trade(t)

                if t.u_id in self._invalid_trades:
                    # User fixed trade and it's now valid.
                    self._invalid_trades.pop(t.u_id)
                    t.highlight_tactic('white')

                self._trades[key] = t

        # Remove manually-closed/unqualified trades.
        remove_keys = [k for k in self._trades.keys()
                       if k not in current_keys]
        [self._closed.append(self._trades.pop(k).key) for k in remove_keys]

        # Remove invalid Trades (at least temporarily)
        # TODO: Determine how they don't get added back in?
        invalid_keys = [k for k, v in self._trades.items()
                        if v.valid is False]
        self._invalid_trades.update({k: self._trades.pop(k) for k in invalid_keys})

        # Add partial exit Trade(s) to parent Trade(s)
        self.sync_partial_exits(rows)

        log.debug("Syncing trades: OK ({} total)".format(len(self._trades)))

        return self._trades

    def sync_partial_exits(self, rows=None):
        log.debug("Syncing partial exits.")

        if not rows:
            rows = get_data_entry_rows()

        partials = [Trade.from_gsheet_row(row, self) for row in rows
                    if 0 < utils.ensure_int_from_pct(row[9]) < 100]

        for trade in self._trades.values():
            matches = [p for p in partials if p.key == trade.key]
            if len(trade.partial_exits) == len(matches):
                continue
            elif trade.partial_exits:
                trade.partial_exits.clear()
            trade.partial_exits.extend(matches)

        log.debug("Syncing partial exits: OK.")


def run_ib_app():
    from time import sleep
    thread = IbAppThreaded()
    thread.start()
    while True:
        sleep(30)


def _test_partial_exit_sync():
    sheet = TradeSheet()
    trades = sheet.trades.values()
    w_partials = [t for t in trades if t.partial_exits]
    if w_partials:
        print(w_partials)
    else:
        print("No active partial trades found.")


def _test_partial_close():
    sheet = TradeSheet()
    trade = list(sheet.trades.values())[0]
    trade.close_partial('1.00', '1/1/2019 5:30')
    sheet.sync_trades(force=True)
    assert trade.partial_exits


def _test_full_close():
    sheet = TradeSheet()
    trade = list(sorted(sheet.trades.values()))[0]
    trade.close(1.00, '1/14/2019 9:10 AM')
    print("Closed trade: {}".format(trade))


def _test_tactic_highlight():
    sheet = TradeSheet()
    trade = list(sheet.trades.values())[0]
    trade.highlight_tactic(bg_color='red')
    print("Highlighted trade: {}".format(trade))
    from time import sleep
    sleep(5)
    trade.highlight_tactic(bg_color='white')


def _test_trade_class():
    t = Trade()
    t.symbol = 'AAPL'
    t.size = 1
    t.date_entered = datetime.now()
    t.entry_price = -1.00
    t.target_price = '152.20, 153.50, $154.50'
    t.target_price1, t.target_price2, t.target_price3 = utils.get_prices_list(t.target_price, count=3)
    t.stop_price = '149.98, $149.00'
    t.stop_price1, t.stop_price2 = utils.get_prices_list(t.stop_price, count=2)
    t.tactic = "JAN 20 $151C"
    t.u_id = utils.get_uid()
    t.parse_tactic()

    assert t.valid
    assert t.number_of_stops == 2, t.number_of_stops
    assert t.number_of_targets == 3, t.number_of_targets
    assert t.stop_price1 == 149.98
    assert t.stop_price2 == 149.00
    assert t.target_price1 == 152.20
    assert t.target_price2 == 153.50
    assert t.target_price3 == 154.50
    assert t.pct_left == 100

    # Proves the math on $1000 capital (size=1) @ $100/contract (entry_price*100).
    assert abs(t.get_target_size()) == 3, t.get_target_size()
    assert abs(t.get_stop_size()) == 5, t.get_stop_size()




def _test_utils_for_ib():
    assert utils.ensure_int_from_pct('100.00%') == 100
    assert utils.ensure_int_from_pct('50.00%') == 50
    assert utils.ensure_int_from_pct('') == 0
    assert utils.ensure_int_from_pct('ABC') == 0


def _run_tests():
    #_test_utils_for_ib()
    #_test_partial_close()
    # _test_partial_exit_sync()
    #_test_full_close()
    #_test_trade_class()
    #_test_tactic_highlight()
    return


if __name__ == '__main__':
    run_ib_app()
    #_run_tests()
