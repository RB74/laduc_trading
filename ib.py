import os
import utils
import gspread
import logging
import cachetools
from time import sleep
from threading import Thread
from collections import defaultdict
from datetime import datetime, timedelta
from ibtrade import Trade
from ibapi import wrapper
from ibapi.tag_value import TagValue
from ibapi.order import Order
from ibapi.client import EClient
from ibapi.utils import iswrapper
from ibapi.ticktype import TickTypeEnum
from ibutils import Contract, get_ratio2
from ibapi.execution import Execution, ExecutionFilter


config = utils.config
LOG_PATH = os.path.join(utils.LOG_DIR, 'ib.log')
CLOSE_OPEN_ON_START = config['ib'].getboolean('close_open_positions_on_start', False)
TRADE_AFTER_HOURS = config['ib'].getboolean('trade_after_hours', False)
MAP_10_SEC = cachetools.TTLCache(100*100, 10)
MAP_30_SEC = cachetools.TTLCache(100*100, 10)
MAP_30_MIN = cachetools.TTLCache(100*100, 30*60)

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


def now_is_rth():
    try:
        return MAP_30_SEC['OUTSIDE_RTH']
    except KeyError:
        MAP_30_SEC['OUTSIDE_RTH'] = o = utils.now_is_rth()
        return o


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
                 eval_freq=30*1,
                 execute_trades=None):
        Wrapper.__init__(self)
        Client.__init__(self, wrapper=self)
        settings = settings if settings is not None else utils.read_json(SETTINGS_FILE)
        if execute_trades is None:
            execute_trades = config['ib'].getboolean('execute_trades', False)
        if trade_sheet is None:
            from ibtrade import TradeSheet
            self.trade_sheet = TradeSheet(self)

        self.settings = settings
        self.trade_sheet = trade_sheet
        self.test = test if test else settings.get('test_mode', test)
        self.subscribe = subscribe
        self.eval_freq = eval_freq
        self.execute_trades = execute_trades

        self.nextValidOrderId = None
        self.account_id = settings['account_id_test'] if self.test else settings['account_id_production']
        self.balances = defaultdict(dict)
        self.portfolio = defaultdict(dict)
        self.prices = defaultdict(dict)
        self._orphaned_positions = dict()

        self._acc_download_first_cycle = True
        self._connected = False
        self._contracts_by_req = dict()
        self._reqs_by_contract_key = dict()
        self._eval_time = None
        self._combo_legs = dict()
        self._contract_ids = dict()
        self._contract_keys_by_req = dict()
        self._contract_success = dict()
        self._contract_details = defaultdict(list)
        self._trades_w_order = dict()
        self._callbacks = dict()
        self.executions = defaultdict(list)

    @property
    def eval_time(self):
        return self._eval_time

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

    @staticmethod
    def send_ib_trade_fail_error_msg(trade):
        subject = "Laduc-IB Error: {}".format(trade.symbol)
        msg = "There was a problem closing a non-existent trade in Interactive Brokers for " \
              "{}. ".format(trade)
        log.debug(msg)
        msg += "The trade will still be closed in the DE sheet (if possible). " \
               "Check the logs for more information."
        utils.send_notification(subject, msg, to='zekebarge@gmail.com')

    @staticmethod
    def send_close_orphaned_position_msg(portfolio_data: dict):
        c_key = getattr(portfolio_data.get('contract', ''), 'key', '')
        subject = "Laduc-IB Error: {}".format(c_key)
        msg = "The following trade is orphaned within Interactive Brokers " \
              "and will now be closed: {}.".format(portfolio_data)
        log.debug(msg)
        msg += "Check the logs for more details."
        utils.send_notification(subject, msg, to='zekebarge@gmail.com')

    @staticmethod
    def send_close_trade_0_error(trade, price, qty, timestamp):
        subject = "Laduc-IB Error: Closing Trade {}".format(trade.key)
        msg = "The following trade will not be executed in Interactive Brokers" \
              "Because the price or qty is 0/null. The trade will still be closed in GSheets (if possible)" \
              "{} & closing price: {} & closing qty: {} & closing time: {}. " \
              ".".format(trade, price, qty, timestamp)
        log.debug(msg)
        msg += "Check the logs for more details."
        utils.send_notification(subject, msg, to='zekebarge@gmail.com')

    @iswrapper
    def nextValidId(self, orderId: int):
        self.nextValidOrderId = orderId
        if self.subscribe:
            self.reqAccountUpdates(True, self.account_id)
            self.request_executions()
            self.trade_sheet.sync_trades()

    @iswrapper
    def contractDetails(self, req_id, details):

        try:
            leg = self._combo_legs.pop(req_id)
            leg.conId = details.underConId
            contract_key = self._contract_keys_by_req[req_id]
            self._contract_ids[contract_key] = details.underConId

            callback = self._callbacks.pop(req_id, None)
            if callable(callback):
                callback(contract_key, details.underConId)

            # Check parent's legs..if they all have a conId then reqMktData.
            parent = getattr(leg, '__parent_contract', None)
            if parent is None:
                log.error("__parent_contract should've been assigned to option leg but wasn't.")
            else:
                self._register_contract(parent)
        except KeyError:
            pass

    def request_executions(self, filter=False):
        try:
            return MAP_10_SEC['EXECUTION_REQUEST']
        except KeyError:
            MAP_10_SEC['EXECUTION_REQUEST'] = True

        if self._trades_w_order and filter:
            min_time = min(self._trades_w_order.values(),
                           key=lambda x: x.closing_order_time_placed).closing_order_time_placed
            exec_filter = ExecutionFilter()
            # TODO: Fix error here. If Needed?
            exec_filter.time = min_time.strftime('%Y%m%d %H:%M')
        else:
            exec_filter = ExecutionFilter()
        self.reqExecutions(self.next_id(), exec_filter)

    def request_leg_id(self, contract, combo_leg, callback=None):
        try:
            combo_leg.conId = self._contract_ids[contract.key]
        except KeyError:
            pass

        req_id = self.next_id()
        self._contract_keys_by_req[req_id] = contract.key
        self._combo_legs[req_id] = combo_leg
        self._callbacks[req_id] = callback
        self.reqContractDetails(req_id, contract)
        return req_id

    def request_contract_id(self, contract, callback=None):
        req_id = self.next_id()
        self._contract_keys_by_req[req_id] = contract.key
        self._contracts_by_req[req_id] = contract.key
        self._callbacks[req_id] = callback
        self.reqContractDetails(req_id, contract)
        return req_id

    def register_trade(self, trade, force=False):
        """
        Registers a new Trade with IB.
        Possibly requests pricing data for Stock and/or options.

        :param trade: (Trade) The Trade to be registered.
        :return: (None)
        """
        contract = trade.get_contract()
        if not force:
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
        if not now_is_rth():
            log.debug("Skipping market data subscription sync outside of RTH.")
            return False

        check_time = datetime.now() - timedelta(seconds=180)
        contract_dates = self._contract_success.copy()
        for contract_key, last_seen in contract_dates.items():
            matches = self.trade_sheet.get_trades_by_contract_key(contract_key)

            if matches and not last_seen:
                oldest_date = min(matches, key=lambda t: t.date_entered).date_entered
                if self.trade_sheet.init_time < datetime.now() - timedelta(minutes=30)\
                        and oldest_date > datetime.now() - timedelta(minutes=8):
                    # Never successfully seen price on this contract.
                    for trade in matches:
                        if trade.key not in self.trade_sheet.invalid_trades:
                            # highlight entry price.
                            trade.highlight_cell(9, bg_color='red')
                            self.trade_sheet.invalid_trades[trade.key] = trade

            elif not matches or (last_seen and last_seen < check_time):
                # Last price was seen too long ago
                # OR we're getting prices for an already closed trade.
                # Cancel market data subscriptions unless it's
                # the only subscription we have left.
                # Trade evaluations occur based on IbApp.tickPrice()
                if len(self._reqs_by_contract_key) == 1:
                    continue

                if not matches:
                    # No trades under this contract.
                    # Are there trades under the symbol?
                    try:
                        req_id = self._reqs_by_contract_key[contract_key]
                        contract = self._contracts_by_req[req_id]
                        matches = self.trade_sheet.get_trades_by_symbol(contract.symbol)
                        if matches:
                            # Keep the subscription until
                            # The trade closes.
                            continue
                    except KeyError:
                        pass

                # Lose the market data subscription.
                try:
                    popped_contract = self._reqs_by_contract_key.pop(contract_key)
                    self.cancelMktData(popped_contract)
                    log.debug("Cancelled market data for {}".format(contract_key))
                except (KeyError, ConnectionAbortedError):
                    log.error("{}: Error cancelling market data "
                              "subscription".format(contract_key))
                    if not self.isConnected():
                        self.connect()

                # Lose the contract success.
                try:
                    self._contract_success.pop(contract_key)
                except KeyError:
                    pass

    def place_order(self, contract=None, order=None, qty=None, action=None, trade=None):
        """
        Places an market order to IB.

        :param contract:
        :param order:
        :param qty:
        :param action: (str, default 'BUY') options = ('BUY', 'SELL')
        :return:
        """
        if trade is not None:
            # Avoid double triggers.
            if trade.last_execution and trade.last_execution > datetime.now() - timedelta(seconds=180):
                return log.error("Cannot place an order twice on the same trade in the last 180 seconds.")
            if trade.locked:
                return log.error("Cannot place an order on an already locked trade: {}".format(trade))
            if not trade.valid:
                return log.error("Cannot place an order on an invalid trade: {}".format(trade))

            if not trade.entry_price:
                if qty is None:
                    qty = trade.get_open_size()
                if action is None:
                    action = trade.opening_side
            else:
                if action is None:
                    action = trade.closing_side

            og_qty = qty
            qty = self.get_checked_order_qty(trade, qty, action)
            if qty == 0:
                log.debug("Closing order qty unavailable.")
                if action == trade.closing_side:
                    # This trade should be getting closed
                    # but we have something wrong in IB.
                    self.send_ib_trade_fail_error_msg(trade)

                    # Still close the trade in GSheets.
                    price = self.get_midpoint_by_symbol(
                        trade.symbol,
                        contract_key=trade.get_contract().key,
                        sec_type=trade.sec_type.lower())

                    self.close_trade(trade, price, og_qty, execution=True)

                return False

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

        # Purposely not updating BAG contracts here.
        # It was causing errors 1/18/2019
        if contract.secType == 'BAG1':
            order.smartComboRoutingParams = []
            order.smartComboRoutingParams.append(TagValue("NonGuaranteed", "1"))
            # Assumes contract was built from
            # ib.get_bag_contract()
            if not getattr(order, 'orderComboLegs', []):
                order.orderComboLegs = contract.comboLegs.copy()
            if not all([getattr(c, 'price', None) for c in order.orderComboLegs]):
                order.randomizePrice = True
        if TRADE_AFTER_HOURS and not now_is_rth():
            order.outsideRth = True

        log.info("api.place_order({}) >> {} {} "
                 "{}".format(order_id, order.action, order.totalQuantity, contract.key))

        super().placeOrder(order_id, contract, order)

        if trade is not None:
            trade.orders[order_id] = order
            self._trades_w_order[order_id] = trade
            trade.last_execution = trade.closing_order_time_placed = datetime.now()
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
            bid_time = prices.get(bid_key + '_time', None)
            ask_time = prices.get(ask_key + '_time', None)
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

        if not TRADE_AFTER_HOURS and not now_is_rth():
            return

        # Store price
        contract = self._contracts_by_req[req_id]
        tick_type = tick_type_map.get(tick_type, tick_type)
        key1 = '{}_{}'.format(contract.secType, tick_type).lower()
        self.prices[req_id][key1] = float(price)
        self.prices[req_id][key1 + '_time'] = datetime.now()

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

        try:
            return MAP_30_SEC['EVALUATE_TRADES_TRIGGER']
        except KeyError:
            MAP_30_SEC['EVALUATE_TRADES_TRIGGER'] = True
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
        #eval_time = self._eval_time if self._eval_time else self.trade_sheet.init_time
        #if eval_time > datetime.now() - timedelta(seconds=self.eval_freq):
        #    # Don't evaluate trades until interval has passed.
        #    return

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
        # Avoid double triggers.
        if trade.last_execution and trade.last_execution > datetime.now() - timedelta(seconds=60):
            return False
        trade.last_execution = datetime.now()

        # Get Stock bid/ask to evaluate stop/targets.
        prices = self.get_price_data_by_symbol(
            trade.symbol,
            sec_type_key=None if trade.sec_type in ('CASH', 'STK') \
                    else (trade.sec_type, trade.get_contract().key)
        )
        if prices is None:
            return False

        try:
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

        target_reached = (trade.profits_up and price >= target_price and price > trade.entry_price)\
                      or (trade.profits_down and price <= target_price and price < trade.entry_price)

        if not stop_price:
            stop_reached = False
        else:
            stop_reached = (trade.profits_up and price <= trade.stop_price and price < trade.entry_price)\
                        or (trade.profits_down and price >= trade.stop_price and price > trade.entry_price)

        if target_reached or stop_reached:

            if trade.sec_type in ('OPT', ):
                try:
                    key = trade.sec_type.lower()
                    opt_bid = prices[key + '_bid']
                    opt_ask = prices[key + '_ask']
                except KeyError:
                    log.error("IbApp.evaluate_trade() error getting opt_bid/opt_ask: "
                              "{}\nprices data: {}".format(trade, prices))
                    return None
                mkt_price = opt_bid if trade.closing_side == 'SELL' else opt_ask
                price = round(mkt_price, 2) if self.execute_trades else (opt_bid + opt_ask) / 2

            elif self.execute_trades:
                mkt_price = stk_bid if trade.is_long > 0 else stk_ask
                price = round(mkt_price, 2)

            if target_reached:
                close_reason = TGT_REACHED
                close_size = abs(trade.get_target_size())

            else:
                close_reason = STOP_LOSS
                close_size = abs(trade.get_stop_size())

            trade.close_reason = close_reason
            # NOTE: BAG trades are closed with the stock price.
            # Until I figure out how to stream the BAG contract price.
            self.close_trade(trade, price, close_size)

    def get_checked_order_qty(self, trade, qty, action):
        if action == 'SELL' and trade.is_long:
            # Can we actually sell?
            # Find the portfolio for an open trade
            acc_key = list(self.portfolio.keys())[0]
            portfolio = self.portfolio[acc_key]
            trade_contract = trade.get_contract()
            c_key = trade_contract.key
            try:
                acc_data = portfolio[c_key]
            except KeyError:
                if trade.is_short:
                    return qty
                # No reference to this long position in portfolio...
                # We cannot sell a position we dont have.
                log.debug("Couldn't find trade in portfolio. "
                          "This position cannot be sold: {}".format(trade))
                return 0

            cur_qty = acc_data['position']
            if cur_qty <= 0:
                return qty  # Short more? sure...
            elif cur_qty >= qty:
                return qty  # Sell some/all position? sure...
            elif cur_qty < qty:
                log.debug("Order qty {} is being trimmed to the max available: "
                          "{}: {}".format(qty, cur_qty, trade))
                return cur_qty

        return qty

    def close_orphaned_position(self, data):
        log.debug("Closing orphaned position in IB - doesn't match any trades: {}".format(data))

        self.place_order(data['contract'],
                         qty=abs(data['position']),
                         action='SELL' if data['position'] > 0 else 'BUY')
        self._orphaned_positions[data['contract'].key] = data

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
            log.error("Trade only has {} available yet close_trade qty"
                      " is {}: {}".format(size_open, qty, trade))
            qty = size_open

        if not qty or (not price and trade.sec_type != 'BAG'):
            # No pricing on BAG trades
            # Somehow got a 0 qty/price trade...need to figure
            # out how or at least be notified.
            execution = True
            self.send_close_trade_0_error(trade, price, qty, timestamp)

        if execution is None and self.execute_trades:
            # Expect a call back later with an execution once complete.
            order_id = self.place_order(
                qty=qty,
                action=trade.closing_side,
                trade=trade)
            if not order_id:
                trade.fail_count += 1
                if trade.fail_count > 3:
                    trade.valid = False
                    trade.lock()
                    log.debug("Locking 3x failed trade.".format(trade))
                    return None

        else:
            if timestamp == 'now':
                timestamp = utils.now_est()

            trade.close(price, timestamp)
            self.trade_sheet.close_trade(trade)
            #self.trade_sheet.sync_trades(force=True)

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

    @iswrapper
    def accountDownloadEnd(self, account_name):

        if not now_is_rth():
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

        sync_open = True
        if sync_open:
            # Sync open positions w/ GSheet
            for contract_key, data in portfolio.items():
                if data['position'] == 0:
                    continue

                trades = self.trade_sheet.get_trades_by_contract_key(contract_key)
                if not trades:
                    # Close orphaned IB position
                    # TODO: Attempt to find position in sheet?
                    # Why would it be orphaned?
                    self.close_orphaned_position(data)
                # TODO: Compare trades to position to see if its as expected?
                continue

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
                    self.place_order(contract, qty=diff, action='SELL')
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
        """
        Execution details are stored by request id in the IbApp.executions dictionary.
        The execution is matched to a Trade
        :param req_id:
        :param contract:
        :param execution:
        :return:
        """

        contract = Contract.from_ib(contract)
        self.executions[req_id].append((contract, execution))
        execution.cumQty
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

        # Catch orphaned position and exit early.
        try:

            pos = self._orphaned_positions[contract.key]
            if pos['position'] == execution.cumQty:
                self._orphaned_positions.pop(contract.key)
                self.send_close_orphaned_position_msg(pos)
                return
        except KeyError:
            pass

        _map = {'SELL': 'SLD', 'BUY': 'BOT'}
        matches = self.trade_sheet.get_trades_by_contract_key(contract.key)
        for trade in matches:
            if not trade.locked or execution.orderId not in trade.orders:
                continue

            order = trade.orders[execution.orderId]
            if execution.cumQty < order.totalQuantity:
                # Wait for execution to meet order qty.
                continue

            elif not trade.entry_price:
                # Newly entered trade - Update GSheet with exec price.
                log.debug("Execution for trade open received: {}".format(trade))
                trade.register_entry_price(execution.price)
                self._trades_w_order.pop(execution.orderId)

            elif _map.get(trade.opening_side, None) == execution.side:
                # Interesting...We're opening a position but
                # The position has an entry_price already...
                # TODO: Handle opened position with pre-existing entry price.
                pass

            elif not trade.exit_price:
                # Completed trade: update GSheet/close out.
                log.debug("Execution for trade close received: {}".format(trade))
                self.close_trade(trade, execution.avgPrice, execution.cumQty, execution=execution)
                self._trades_w_order.pop(execution.orderId)
            else:
                continue

            trade.unlock()
        log.debug("EXECUTION: ({order_id}) - "
                  "{action} {quantity} {symbol} @ {price}".format(**data))

    @iswrapper
    def execDetailsEnd(self, req_id: int):
        # We're going to use the latest market price available.
        log.debug("Execution details end for request: {}".format(req_id))

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


def run_ib_app():
    from time import sleep
    thread = IbAppThreaded()
    thread.start()
    while True:
        sleep(30)
        if not now_is_rth():
            continue
        if not thread.app.eval_time:
            continue

        # Make sure trades stay syncing.
        if thread.app.eval_time < datetime.now() - timedelta(seconds=thread.app.eval_freq*2):
            thread.app.trade_sheet.sync_trades(force=True)
            thread.app._eval_time = datetime.now()
            print("force-sync'ed")


def _test_partial_exit_sync():
    from ibtrade import TradeSheet
    sheet = TradeSheet()
    trades = sheet.trades.values()
    w_partials = [t for t in trades if t.partial_exits]
    if w_partials:
        print(w_partials)
    else:
        print("No active partial trades found.")


def _test_partial_close():
    from ibtrade import TradeSheet
    sheet = TradeSheet()
    trade = list(sheet.trades.values())[0]
    trade.close_partial('1.00', '1/1/2019 5:30')
    sheet.sync_trades(force=True)
    assert trade.partial_exits


def _test_full_close():
    from ibtrade import TradeSheet
    sheet = TradeSheet()
    trade = list(sorted(sheet.trades.values()))[0]
    trade.close(1.00, '1/14/2019 9:10 AM')
    print("Closed trade: {}".format(trade))


def _test_tactic_highlight():
    from ibtrade import TradeSheet
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

    ratio_lists = [
        [4, 2, 4, 2],
        [1, 1, 1, 1],
        [1, 2],
        [1, 2, 3, 4],
        [1, 2, 2, 1],
        [1, 2, 2, 2],
        [2, 4],
        [3, 6],
        [4, 8],
        [8, 4],

    ]
    for i, r in enumerate(ratio_lists):
        r2 = get_ratio2(r)
        print("r{}".format(i), r, r2)



def _run_tests():
    _test_utils_for_ib()
    #_test_partial_close()
    # _test_partial_exit_sync()
    #_test_full_close()
    #_test_trade_class()
    #_test_tactic_highlight()
    return


if __name__ == '__main__':
    run_ib_app()
    #_run_tests()
