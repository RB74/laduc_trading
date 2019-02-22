"""
The database layer to Interactive Brokers trade integration.
"""
import ib
import os
import pytz
import utils
import ibutils
import ibtrade
from time import sleep
from threading import Thread
from utils import CACHE_1_SEC
from ibapi.order import Order
from ibapi.contract import ComboLeg
from ibapi.tag_value import TagValue
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from sqlalchemy.orm.exc import StaleDataError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError, OperationalError
from ibtrade import Trade, get_data_entry_trades, MAP_10_SEC
from sqlalchemy.ext.hybrid import hybrid_method, hybrid_property
from ib import IbApp, IbAppThreaded, iswrapper, tick_type_map, log
from sqlalchemy.orm import sessionmaker, scoped_session, relationship
from sqlalchemy import create_engine, select, func, and_, or_, Column, String, Integer, Float, DateTime, ForeignKey


CANCEL_PLACED_ORDERS_ON_START = False
# Set this to true once to cancel placed orders that didnt execute (e.g after hours)

SYNC_POSITIONS = False
# True deletes open trades from IB that don't match an open GSheet trade.

USE_LMT_ORDERS = True
# True will use limit orders to open and close trades.

LMT_PCT_OFFSET = 0
# The percentage away from the bid or ask (or mid when LMT_USING_MID = True) to place the limit order.
# NOTE: Only applies if USE_LMT_ORDERS is True

LMT_USING_MID = True
# True will base limit orders off the mid price.
# False will use the bid on SELL and ask on BUY orders.
# NOTE: Only applies if USE_LMT_ORDERS is True

LMT_TIME_IN_FORCE = 'FOK'
# The time-in-force option used when placing limit orders.
# Default 'FOK' (Fill-or-Kill)

STK_NBBO_OFFSET = 0.02
# The $ away from the NBBO to aggressively move stock order around until it fills.
# NOTE: Only applies of USE_LMT_ORDERS is True

EVAL_ORDER_PAUSE_MINUTES = 5
# The # of minutes to pause between evaluations when a trade has had an order placed.


EVAL_DEBIT_SPREAD_PANIC_CLOSE_VALUE = 0.02
# The price at which a debit spread is closed
# Prevents turning into a credit spread.

POSITION_SIZE_FACTOR = 1000
# 1 = $1000 (default)
# Multiplies SIZE by POSITION_SIZE_FACTOR to determine total USD
# capital allocated to the trade.


class OrderStatus:
    READY = 'ready'
    PLACED = 'placed'
    COMPLETE = 'complete'
    ERROR = 'error'
    PENDING_STATUSES = [READY, PLACED]


class TradeStatus:
    PRE_OPEN_CHECK = 'pre-open-check'
    OPEN = 'open'
    CLOSED = 'closed'
    ERROR = 'error'


class MsgStatus:
    OPEN = 'open'
    RESOLVED = 'resolved'
    UNKNOWN = 'unknown'


DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
DB_PATH = os.path.join(DATA_DIR, 'ib.db')
EVAL_INTERVAL = utils.config['ib'].getint('eval_interval', 30)
TEST_MODE = utils.config['ib'].getboolean('test_mode', True)

Base = declarative_base()

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

engine = create_engine("sqlite:///" + DB_PATH, echo=False)
Session = scoped_session(sessionmaker(bind=engine))


class IbMktDataSubscription(Base):
    __tablename__ = 'ib_mkt_data_subscriptions'
    id = Column(Integer, primary_key=True)
    contract_id = Column(String, unique=True)
    ib_contract_id = Column(Integer)
    date_added = Column(DateTime, default=datetime.utcnow)
    date_active = Column(DateTime)
    active = Column(Integer, default=0)
    valid = Column(Integer, default=1)
    request_id = Column(Integer, unique=True)
    date_requested = Column(DateTime)

    @hybrid_method
    def cancel_subscription(self, ib_app):
        if self.request_id:
            log.debug("IBMktDataSubscription: Cancel {}".format(self.contract_id))
            ib_app.cancelMktData(self.request_id)
            self.request_id = None
            self.date_requested = None
            self.active = 0

    @hybrid_method
    def request_subscription(self, ib_app, contract: ibutils.Contract):
        log.debug("IBMktDataSubscription: Request {}".format(self.contract_id))
        self.active = 1
        self.request_id = ib_app.register_contract(contract)
        self.date_requested = datetime.utcnow()

    @hybrid_method
    def refresh_subscription(self, ib_app, contract):
        self.cancel_subscription(ib_app)
        self.request_subscription(ib_app, contract)


class IbTrade(Base):
    """GSheet Trades"""
    __tablename__ = 'ib_trades'

    id = Column(Integer, primary_key=True)
    alert_category = Column(String)
    symbol = Column(String)
    exchange = Column(String, default='SMART')
    size = Column(Float)
    sec_type = Column(String)
    expiry_month = Column(Integer)
    expiry_day = Column(Integer)
    expiry_year = Column(Integer)
    strike = Column(Float)
    tactic = Column(String)
    underlying_entry_price = Column(Float)
    original_entry_price = Column(Float)
    stop_price = Column(String)
    stop_price1 = Column(Float)
    stop_price2 = Column(Float)
    target_price = Column(String)
    target_price1 = Column(Float)
    target_price2 = Column(Float)
    target_price3 = Column(Float)
    entry_price = Column(Float)
    pct_sold = Column(Integer)
    exit_price = Column(Float)
    underlying_exit_price = Column(Float)
    date_entered = Column(DateTime)
    date_exited = Column(DateTime)
    date_updated = Column(DateTime, default=datetime.utcnow)
    status = Column(String, default=TradeStatus.OPEN)
    u_id = Column(String)

    parent_trade_id = Column(Integer, ForeignKey('ib_trades.id'))
    contract_id = Column(String)
    underlying_contract_id = Column(String)
    contract_pk = Column(Integer, ForeignKey('ib_contracts.id'))
    underlying_contract_pk = Column(Integer, ForeignKey('ib_contracts.id'))
    date_added = Column(DateTime, default=datetime.utcnow)
    registration_attempts = Column(Integer, default=0)

    legs = relationship("IbTradeLeg")
    orders = relationship("IbOrder", back_populates='trade')
    contract = relationship('IbContract', foreign_keys=[contract_pk])
    underlying_contract = relationship('IbContract', foreign_keys=[underlying_contract_pk])
    parent_trade = relationship('IbTrade')
    messages = relationship('IbTradeMessage', back_populates='trade')

    @hybrid_property
    def profits_down(self):
        return self.target_price1 < self.underlying_entry_price

    @hybrid_property
    def profits_up(self):
        return self.target_price1 > self.underlying_entry_price

    @hybrid_property
    def gsheet_trade(self):
        return Trade.from_ib_trade(self)

    @hybrid_property
    def has_pending_order(self):
        return len([o for o in self.orders
                    if o.status in OrderStatus.PENDING_STATUSES]) > 0

    @hybrid_property
    def has_opening_order(self):
        action = 'BUY' if self.is_long else 'SELL'
        return len(self.get_orders_by_action(action)) > 0

    @hybrid_property
    def has_opening_order_complete(self):
        action = 'BUY' if self.is_long else 'SELL'
        return len(self.get_orders_completed_by_action(action)) > 0

    @hybrid_property
    def has_valid_legs(self):
        if self.sec_type != 'BAG':
            return True
        legs = self.legs
        valid_legs = [leg.ib_contract_id for leg in legs if leg.ib_contract_id]
        return len(valid_legs) == len(legs)

    @hybrid_property
    def is_short(self):
        if self.sec_type == 'BAG':
            return False
        return self.size < 0

    @hybrid_property
    def is_long(self):
        if self.sec_type == 'BAG':
            return True
        return self.size > 0

    @hybrid_property
    def left_qty(self):
        # Amount of shares left to close the trade.
        if self.is_short:
            return self.total_qty - self.bought_qty
        else:
            return self.total_qty - self.sold_qty

    @hybrid_property
    def total_qty(self):
        """
        Returns the total open quantity.
        Defaults to the opening order if it exists.
        Falls back to the IbTrade.original_entry price if it exists.
        Falls back to the IbTrade.entry_price (if no original)
        Returns 0 if default and fallbacks don't exist.
        """
        orders = self.get_orders_by_action('BUY' if self.is_long else 'SELL')
        if orders:
            return sum([o.qty for o in orders])

        size = self.size
        entry = self.original_entry_price or self.entry_price

        if not all((entry, size)):
            return None
        if self.sec_type in ('BAG', 'OPT'):
            entry *= 100

        return round(abs((size*1000)/entry), 0)

    @hybrid_property
    def stop_qty(self):
        # Number of shares to stop out with.
        session = Session.object_session(self)
        targets, stops = self.get_target_and_stop_orders(session)
        expected_execs = len(self.stop_prices)

        if targets or len(stops) == expected_execs - 1:
            return self.left_qty
        return round(self.total_qty/expected_execs, 0)

    @hybrid_property
    def target_qty(self):
        # Number of shares to close
        session = Session.object_session(self)
        targets, stops = self.get_target_and_stop_orders(session)
        expected_execs = len(self.target_prices)

        if stops or len(targets) == expected_execs - 1:
            return self.left_qty
        return round(self.total_qty/expected_execs, 0)

    @hybrid_property
    def bought_qty(self) -> float:
        # Number of shares bought
        return sum([abs(order.qty) for order in self.get_orders_completed_by_action('BUY')])

    @hybrid_property
    def sold_qty(self) -> float:
        # Number of shares sold
        return sum([abs(order.qty) for order in self.get_orders_completed_by_action('SELL')])

    @hybrid_property
    def target_prices(self) -> list:
        prices = list()
        for price in (self.target_price1, self.target_price2, self.target_price3):
            if price:
                prices.append(price)
        return prices

    @hybrid_property
    def stop_prices(self) -> list:
        prices = list()
        for price in (self.stop_price1, self.stop_price2):
            if price:
                prices.append(price)
        return prices

    @hybrid_method
    def get_ib_execution_contract(self):
        contract = self.contract
        if not contract:
            return None
        return contract.get_ib_contract(self.id)

    @hybrid_method
    def get_ib_execution_contract_underlying(self):
        contract = self.underlying_contract
        if not contract:
            return None
        return contract.get_ib_contract(self.id)

    @hybrid_method
    def get_orders_by_action(self, action):
        return [order for order in self.orders
                if order.action == action
                and order.status != OrderStatus.ERROR
                and order.exclude == 0]

    @hybrid_method
    def get_orders_completed_by_action(self, action):
        return [order for order in self.orders
                if order.action == action
                and order.status == OrderStatus.COMPLETE
                and order.exclude == 0]

    @hybrid_method
    def get_target_and_stop_orders(self, session) -> (list, list):
        """
        Returns a list of target order/executions and a list of stop order/executions.
        Each object within each list is a tuple like
            (IbOrder, IbExecution)
        OR (for BAG trades)
            (IbOrder, list(IbExecution1, IbExecution2,..))
        :return:
        """
        action = 'BUY' if self.is_short else 'SELL'
        orders = self.get_orders_completed_by_action(action)
        orders_desc = sorted(orders, key=lambda o: o.request_id, reverse=True)
        targets, stops = list(), list()

        for order in orders_desc:
            e = order.get_last_execution()

            if e is None:
                continue

            if self.sec_type == 'BAG':
                e = order.get_valid_executions(get_executions_by_order_id(session, order.request_id))
                exit = order.get_executed_price(e)
            else:
                exit = e.avg_price

            if self.is_short:
                if abs(exit) < self.entry_price:
                    targets.append((order, e))

                else:
                    stops.append((order, e))

            elif self.is_long:
                if self.sec_type == 'BAG':
                    if self.size < 0:
                        diff = exit - -self.entry_price
                        if diff > 0:
                            targets.append((order, e))
                        else:
                            stops.append((order, e))
                        continue

                if abs(exit) >= self.entry_price:
                    targets.append((order, e))
                else:
                    stops.append((order, e))

        return targets, stops

    @hybrid_method
    def get_next_target(self):
        action = 'BUY' if self.is_short else 'SELL'
        orders = self.get_orders_by_action(action)
        idx = len(orders)

        try:
            return idx, self.target_prices[idx]
        except IndexError:
            return None, None

    @hybrid_method
    def get_next_stop(self):
        action = 'BUY' if self.is_short else 'SELL'
        orders = self.get_orders_by_action(action)
        idx = len(orders)
        try:
            return idx, self.stop_prices[idx]
        except IndexError:
            return None, None

    def __repr__(self):
        return "<IbTrade(con_id='{}', ul_entry='{}', target1='{}', stop1='{}', entry_price='{}', " \
               "exit_price='{}', ul_exit='{}', status='{}', entry_date='{}', pos_size='{}')>".format(
                    self.contract_id,
                    self.underlying_entry_price,
                    self.target_price1,
                    self.stop_price1,
                    self.entry_price,
                    self.exit_price,
                    self.underlying_exit_price,
                    self.status,
                    self.date_entered,
                    self.total_qty)


class IbTradeMessage(Base):
    __tablename__ = 'ib_trade_messages'
    id = Column(Integer, primary_key=True)
    trade_id = Column(Integer, ForeignKey('ib_trades.id'))
    text = Column(String)
    text_public = Column(String)
    status = Column(String, default='open')
    date_added = Column(DateTime, default=datetime.utcnow)
    date_resolved = Column(DateTime)
    date_last_occured = Column(DateTime, default=datetime.utcnow)
    count = Column(Integer, default=1)
    error_code = Column(Integer)
    request_id = Column(Integer)
    trade = relationship('IbTrade', back_populates='messages')

    def __repr__(self):
        return """IbTradeMessage(trade_id={}, text={}, count={}, error_code={}, date_added={}, date_resolved={}""".format(
            self.trade_id, self.text, self.count, self.error_code, self.date_added, self.date_resolved
        )


class IbPosition(Base):
    """ibapi portfolio records."""
    __tablename__ = 'ib_positions'

    contract_id = Column(String, primary_key=True)
    symbol = Column(String)
    security_type = Column(String)
    position = Column(Float)
    market_price = Column(Float)
    account_name = Column(String, primary_key=True)
    time = Column(DateTime, default=datetime.utcnow)
    valid = Column(Integer, default=1)
    checked = Column(Integer, default=0)

    @hybrid_method
    def get_guessed_ib_contract(self) -> ibutils.Contract:
        sec_type = self.security_type
        if sec_type == 'CASH':
            return ibutils.get_cash_contract(self.symbol)
        elif sec_type == 'STK':
            return ibutils.get_stock_contract(self.symbol)
        elif sec_type == 'OPT':
            return ibutils.get_option_contract_from_contract_key(self.contract_id)

    def __repr__(self):
        return """<IbPosition(contract_id='{}', position='{}', time='{}'""".format(
            self.contract_id, self.position, self.time)


class IbContract(Base):
    """ibapi Contracts"""
    __tablename__ = 'ib_contracts'

    id = Column(Integer, primary_key=True)
    contract_id = Column(String)
    ib_contract_id = Column(Integer)
    symbol = Column(String)
    exchange = Column(String, default='SMART')
    sec_type = Column(String)
    expiration = Column(String)
    strike = Column(Float)
    right = Column(String)   # C, P
    trade_legs = relationship('IbTradeLeg')

    @hybrid_property
    def ib_contract(self) -> ibutils.Contract:
        """Risky way to retrieve IB contract (possibly raises error on BAG contracts)"""
        if self.sec_type == 'STK':
            return ibutils.get_stock_contract(self.symbol, self.exchange)
        elif self.sec_type == 'OPT':
            return ibutils.get_option_contract(
                self.symbol, self.strike, self.expiration, self.right, self.exchange)
        elif self.sec_type == 'BAG':
            return self.get_bag_contract()
        elif self.sec_type == 'CASH':
            return ibutils.get_cash_contract(self.symbol, self.exchange)

    @hybrid_method
    def get_ib_contract(self, trade_id: int) -> ibutils.Contract:
        """Safe way to retrieve a contract (unless you pass trade_id as None)."""
        if self.sec_type == 'BAG':
            return self.get_bag_contract(trade_id)
        return self.ib_contract

    @hybrid_method
    def get_bag_contract(self, trade_id: int = None) -> ibutils.Contract:
        c = ibutils.Contract()
        c.secType = 'BAG'
        c.currency = 'USD'
        c.exchange = self.exchange
        c.comboLegs = list()
        symbols = list()
        criteria = [IbTradeLeg.contract_id == self.contract_id]
        if trade_id:
            criteria = IbTradeLeg.trade_id == trade_id
        else:
            criteria = criteria[0]
        session = Session.object_session(self)
        legs = session.query(IbTradeLeg).filter(criteria).all()

        if not trade_id:
            trade_ids = list(set([leg.trade_id for leg in legs]))
            if len(trade_ids) > 1:
                raise AttributeError("IbContract {} has multiple trade_ids associated. "
                                     "Provide the trade_id param to IbContract.get_bag_contract() "
                                     "to avoid this error.".format(self))

        for leg in sorted(legs, key=lambda t: t.sequence):
            ib_leg = leg.ib_combo_leg
            c.comboLegs.append(ib_leg)

        if symbols:
            c.symbol = ','.join(list(set(symbols)))
        else:
            c.symbol = self.symbol

        return c

    def __repr__(self):
        return """<IbContract(contract_id='{}', id='{}')>""".format(self.contract_id, self.id)


class IbPrice(Base):
    """ibapi tickPrices"""
    __tablename__ = 'ib_prices'

    contract_id = Column(String, nullable=False, primary_key=True)
    time = Column(DateTime, nullable=False, primary_key=True)
    price = Column(Float, nullable=False, primary_key=True)
    bid = Column(Float)
    ask = Column(Float)


class IbExecution(Base):
    """ibapi Executions"""
    __tablename__ = 'ib_executions'

    exec_id = Column(String, primary_key=True)
    base_exec_id = Column(String)
    order_id = Column(Integer)
    client_id = Column(Integer)
    correction_id = Column(Integer)
    server_time = Column(String)
    utc_time = Column(DateTime)
    acc_number = Column(String)
    exchange = Column(String, default='SMART')
    side = Column(String)
    shares = Column(Float)
    price = Column(Float)
    cum_qty = Column(Float)
    avg_price = Column(Float)
    ib_contract_id = Column(Integer)
    contract_id = Column(String)

    def __repr__(self):
        return """<IbExecution(contract_id='{}', price='{}', shares='{}', cum_qty='{}', server_time='{}')>""".format(
            self.contract_id, self.price, self.shares, self.cum_qty, self.server_time
        )


class IbOrder(Base):
    """ibapi Orders"""
    __tablename__ = 'ib_orders'

    id = Column(Integer, primary_key=True)
    trade_id = Column(Integer, ForeignKey('ib_trades.id'))
    contract_id = Column(String)
    request_id = Column(Integer)
    u_id = Column(String)
    time = Column(DateTime)

    symbol = Column(String)
    action = Column(String)
    offset = Column(Float)
    price = Column(Float)
    qty = Column(Integer)
    tif = Column(String)
    type = Column(String, default='MKT')
    status = Column(String)
    date_added = Column(DateTime, default=datetime.utcnow)
    date_filled = Column(DateTime)
    trade = relationship("IbTrade", back_populates='orders')
    exclude = Column(Integer, default=0)

    @hybrid_property
    def contract(self):
        session = Session.object_session(self)
        contract = session.query(IbContract).filter(
            IbContract.contract_id == self.contract_id
        ).first()
        return contract

    @hybrid_property
    def ib_order(self) -> Order:
        o = Order()
        o.totalQuantity = abs(self.qty or 0)
        o.action = self.action
        o.orderType = self.type
        if self.offset:
            o.auxPrice = self.offset
        if self.price:
            o.lmtPrice = self.price
        if self.trade.sec_type == 'BAG':
            o.smartComboRoutingParams = []
            o.smartComboRoutingParams.append(TagValue("NonGuaranteed", "1"))
        o.tif = self.tif or ''
        return o

    @hybrid_method
    def get_last_execution(self):
        session = Session.object_session(self)
        return session.query(IbExecution).filter(
            IbExecution.order_id == self.request_id
        ).order_by(IbExecution.utc_time.desc()).first()

    @hybrid_method
    def get_valid_executions(self, order_execs) -> list:
        """Returns unique executions with the latest correction"""

        execs = defaultdict(list)
        for e in order_execs:
            execs[e.base_exec_id].append(e)

        valid = list()
        for base_id, store in execs.items():
            latest = list(sorted(store, key=lambda x: x.utc_time))[-1]
            valid.append(latest)

        return valid

    @hybrid_method
    def get_executed_qty(self, execs):
        if not execs:
            return 0
        trade = self.trade
        if trade and trade.sec_type == 'BAG':
            return self.get_executed_bag_qty(execs, trade)
        latest_exec = list(sorted(execs, key=lambda x: x.cum_qty))[-1]
        return latest_exec.cum_qty

    @hybrid_method
    def get_executed_bag_qty(self, execs, trade):
        """Return IbOrder.qty or 0"""
        for leg in trade.legs:
            sub_execs = [b for b in execs
                         if b.contract_id == leg.contract_id
                         and b.cum_qty/leg.ratio == self.qty]
            if not sub_execs:
                return 0

        return self.qty

    @hybrid_method
    def get_executed_price(self, execs):
        """Returns cost / shares for a group of executions."""
        trade = self.trade
        if trade and trade.sec_type == 'BAG':
            return self.get_executed_bag_price(execs, trade)

        latest_exec = list(sorted(execs, key=lambda x: x.cum_qty))[-1]
        return latest_exec.avg_price

    @hybrid_method
    def get_executed_bag_price(self, execs, trade):
        """Returns contract price of multi-leg order."""
        price = 0

        for leg in trade.legs:
            sub_execs = [b for b in execs
                         if b.contract_id == leg.contract_id
                         and b.cum_qty/leg.ratio == self.qty]
            if not sub_execs:
                return 0

            latest_exec = list(sorted(sub_execs, key=lambda x: x.utc_time))[-1]
            sub_avg_price = latest_exec.avg_price*leg.ratio

            if sub_execs[0].side == 'SLD':
                sub_avg_price = -sub_avg_price

            price += sub_avg_price

        return price

    def __repr__(self):
        return """<IbOrder(contract={}, qty={}, id={}, price={}, type={}, action={}, tif={}, time={})>""".format(
            self.contract_id, self.qty, self.request_id, self.price, self.type, self.action, self.tif, self.time
        )


class IbTradeLeg(Base):
    """ibapi ComboLegs"""
    __tablename__ = 'ib_trade_legs'
    id = Column(Integer, primary_key=True)
    trade_id = Column(Integer, ForeignKey('ib_trades.id'))
    exchange = Column(String, default='SMART')
    symbol = Column(String)
    action = Column(String)
    ratio = Column(Integer)
    sequence = Column(Integer)
    contract_id = Column(String)
    contract_pk = Column(Integer, ForeignKey('ib_contracts.id'))
    ib_contract_id = Column(Integer)
    expiration = Column(Integer)
    date_added = Column(DateTime, default=datetime.utcnow)
    date_requested = Column(DateTime)
    registration_attempts = Column(Integer, default=0)
    trade = relationship("IbTrade", back_populates='legs')
    contract = relationship('IbContract', back_populates='trade_legs')

    @hybrid_property
    def ib_combo_leg(self) -> ComboLeg:
        c = ComboLeg()
        c.action = self.action
        c.conId = self.ib_contract_id
        c.ratio = self.ratio
        c.exchange = self.exchange
        return c

    @hybrid_method
    def get_request_contract(self) -> ibutils.Contract:
        # Returns a contract from contract_id: 'NVDA-20190125-150.0-C'
        return ibutils.get_option_contract_from_contract_key(self.contract_id)

    def __repr__(self):
        return """<IbTradeLeg(contract_id='{}', action='{}', ratio='{}', trade_id='{}')>""".format(
            self.contract_id, self.action, self.ratio, self.trade_id)


def call_with_session(func, *args):
    """Executes a function with a Session() as the first parameter."""
    session = Session()

    try:
        result = func(session, *args)
        session.commit()
    except Exception as e:
        session.rollback()
        if TEST_MODE or ibtrade.SHEET_TEST_MODE:
            log.error("{}: {}".format(func.__name__, e))
        raise
    finally:
        session.close()

    return result


def delete_old_prices(session, minutes=20):
    condition = IbPrice.time < datetime.utcnow() - timedelta(minutes=minutes)
    session.query(IbPrice).filter(condition).delete(synchronize_session=False)


def delete_old_positions(session):
    session.query(IbPosition).filter(IbPosition.position == 0).delete(synchronize_session=False)


def delete_trade_legs(session, trade_id):
    session.query(IbTradeLeg).filter(IbTradeLeg.trade_id == trade_id).delete(synchronize_session=False)


def evaluate_trades(session, ib_app, outside_rth=False):
    """Creates an IbOrder for any trade that needs to be closed (partial or full)"""
    order_time = datetime.utcnow() - timedelta(minutes=EVAL_ORDER_PAUSE_MINUTES)

    open_orders = session.query(IbOrder.trade_id).filter(
        IbOrder.status.in_(OrderStatus.PENDING_STATUSES)
    ).all()

    recent_trades = session.query(IbOrder.trade_id).filter(
        and_(IbOrder.date_filled > order_time,
             IbOrder.exclude == 0)
    ).all()

    trade_ids = [o.trade_id for o in open_orders]
    trade_ids.extend([o.trade_id for o in recent_trades])

    trade_criteria = [
        IbTrade.date_exited.is_(None),
        IbTrade.u_id.isnot(None),
        IbTrade.underlying_contract_id.isnot(None),
        IbTrade.entry_price.isnot(None)
    ]
    if trade_ids:
        trade_criteria.append(IbTrade.id.notin_(trade_ids))
    if not utils.now_is_rth():
        trade_criteria.append(IbTrade.sec_type == 'STK')

    trades = session.query(IbTrade).filter(and_(*trade_criteria)).all()

    stocks_open = utils.get_market_stocks_open()
    options_open = utils.get_market_options_open()

    for t in trades:

        p = get_price_by_contract_id(session, t.underlying_contract_id, min_seconds=60*3)
        if p is None:
            continue
        price = p.price

        target_idx, target_price = t.get_next_target()
        stop_idx, stop_price = t.get_next_stop()

        if target_price is None:
            continue

        action, qty, left = None, None, None
        bought = t.bought_qty
        sold = t.sold_qty

        if t.is_long:
            left = abs(bought) - abs(sold)
            if left <= 0:
                continue
            if t.profits_up:
                if price >= target_price:
                    action = 'SELL'
                    qty = t.target_qty
                elif stop_price and price <= stop_price:
                    action = 'SELL'
                    qty = t.stop_qty
            elif t.profits_down:
                if price <= target_price:
                    action = 'SELL'
                    qty = t.target_qty
                elif stop_price and price >= stop_price:
                    action = 'SELL'
                    qty = t.stop_qty

        elif t.is_short:
            left = -abs(sold) + abs(bought)
            if left >= 0:
                continue
            if t.profits_down:
                if price <= target_price:
                    action = 'BUY'
                    qty = t.target_qty
                elif stop_price and price >= stop_price:
                    action = 'BUY'
                    qty = t.stop_qty
            elif t.profits_up:
                if price >= target_price:
                    action = 'BUY'
                    qty = t.target_qty
                elif stop_price and price <= stop_price:
                    action = 'BUY'
                    qty = t.stop_qty

        mkt_open = stocks_open if t.sec_type == 'STK' else options_open
        force_market_close = False

        if not action and t.sec_type == 'BAG' and t.entry_price > 0 and mkt_open:
            cprice = get_trade_limit_price(session, t, 'SELL', ib_app, use_mid=False, offset=0)
            if cprice <= EVAL_DEBIT_SPREAD_PANIC_CLOSE_VALUE:
                action = 'SELL'
                qty = t.left_qty
                force_market_close = True

        if action and qty and mkt_open:
            if abs(qty) > abs(left):
                qty = left
            contract_price = None
            tif = None
            offset = None
            t.underlying_exit_price = price
            
            if USE_LMT_ORDERS and not force_market_close:
                order_type = 'LMT'
                if 'STK' == t.sec_type:
                    tif = 'PEG MID'
                    offset = STK_NBBO_OFFSET
                else:
                    contract_price = get_trade_limit_price(session, t, action, ib_app)
            else:
                order_type = 'MKT'

            register_order(session, t, action, qty, 
                           status=OrderStatus.READY, 
                           tif=tif, 
                           price=contract_price,
                           type=order_type,
                           offset=offset)


def get_api_contract_by_contract_id(session, contract_id) -> (ibutils.Contract, None):
    contract = session.query(IbContract).filter(IbContract.contract_id == contract_id).one_or_none()
    if contract is None or contract.sec_type == 'BAG':
        return None
    return contract.get_ib_contract(None)


def get_trade_diffs(sql_trade, sheet_trade):
    diffs = list()

    columns = ['symbol', 'size', 'expiry_month', 'expiry_day',
               'expiry_year', 'strike', 'tactic', 'alert_category', 'entry_price',
               'target_price1', 'target_price2', 'target_price3', 'stop_price1', 'stop_price2']
    for c in columns:
        sql_val = getattr(sql_trade, c, None)
        sheet_val = getattr(sheet_trade, c, None)
        if sheet_val and not _same_val(sql_val, sheet_val):
            diffs.append((c, sql_val, sheet_val))
    return diffs


def get_executions_by_order_id(session, order_id):
    return session.query(IbExecution).filter(
            IbExecution.order_id == order_id
        ).all()


def get_open_contract_ids(session):
    ids = list()
    exclude_statuses = [TradeStatus.CLOSED, TradeStatus.ERROR]
    trades = session.query(IbTrade).filter(
        and_(IbTrade.status.notin_(exclude_statuses),
             IbTrade.date_exited.is_(None))
    ).all()

    for t in trades:
        ids.append(t.underlying_contract_id)
        if t.sec_type == 'BAG':
            ids.extend([leg.contract_id for leg in t.legs])
        else:
            ids.append(t.contract_id)

    return list(filter(None, set(ids)))


def get_orders_by_contract_id(session, contract_id, hours_ago=24):
    return session.query(IbOrder).filter(
        and_(IbOrder.contract_id == contract_id,
             IbOrder.time >= datetime.utcnow() - timedelta(hours=hours_ago))
    ).all()


def get_order_by_request_id(session, req_id):
    return session.query(IbOrder).filter(IbOrder.request_id == req_id).one_or_none()


def get_price_by_contract_id(session, contract_id, min_seconds=0) -> IbPrice:
    criteria = [IbPrice.contract_id == contract_id]
    if min_seconds > 0:
        eval_time = datetime.utcnow() - timedelta(seconds=min_seconds)
        criteria.append(IbPrice.time >= eval_time)
        condition = and_(*criteria)
    else:
        condition = criteria[0]
    return session.query(IbPrice).filter(condition).order_by(IbPrice.time.desc()).limit(1).first()


def get_trade_by_uid(session, uid) -> IbTrade:
    return session.query(IbTrade).filter(IbTrade.u_id == str(uid)).one_or_none()


def get_trade_limit_price(session, trade, action, ib_app, use_mid=LMT_USING_MID, offset=LMT_PCT_OFFSET):
    """
    Returns a limit price (possibly at an offset) based on the latest prices in ib_prices.

    :param session:
    :param trade:
    :param action: (string, 'BUY', 'SELL')
    :param ib_app:
    :param use_mid: (bool, default ibdb.LMT_USING_MID)
        True uses the mid price to calculate the limit price.
        Otherwise the 'ask' is used for 'BUY' orders and the 'bid' is used for 'SELL' orders.
    :param offset: (float, default ibdb.LMT_PCT_OFFSET)
        BUY: offset=0.05 returns 0.95*price
        short trades: offset=0.05 = 1.05*price
    :return:
    """

    # Determine target price type.
    if use_mid:
        type = 'mid'
    elif action == 'BUY':
        type = 'ask'
    elif action == 'SELL':
        type = 'bid'
    else:
        raise TypeError("Unexpected action '{}', supported actions: ['BUY', 'SELL'].".format(action))

    price = get_trade_market_price(session, trade, ib_app, type=type)

    if not price:
        return 0

    # Adjust price to offset
    # Buy aggressively, pay more & sell aggressively, for less.
    if offset:
        if action == 'BUY':
            offset = 1 + abs(offset)
        else:
            offset = 1 - abs(offset)

        price *= offset

    if trade.sec_type in ('STK', 'BAG', 'OPT'):
        price = round(price, 2)

    return price


def get_trade_market_price(session, trade, ib_app, type='mid', timeout_seconds=60, raise_gsheet_error=True):
    """
    Returns the current market price of a trade (BAG or otherwise)
    :param session:
    :param trade:
    :param ib_app:
    :param type ('mid' or 'price', 'bid', 'ask')
    :param timeout_seconds (int, default 60)
    :param raise_gsheet_error:
    :return:
    """
    if type == 'mid':
        type = 'price'
    _getprice = lambda x: getattr(x, type)

    if trade.sec_type == 'BAG':
        legs = trade.legs
        price = 0
        for leg in legs:
            p = get_price_by_contract_id(session, leg.contract_id, min_seconds=timeout_seconds)
            if p is None:
                contract = leg.get_request_contract()
                sub, p = register_market_data_subscription(session, contract, ib_app)
                if p is None:
                    if raise_gsheet_error:
                        msg = IbTradeMessage()
                        msg.error_code = 99992
                        msg.text = "Failure to retrieve pricing data for {} " \
                                   "within 30 seconds. Try again.".format(contract.key)
                        register_trade_msg(trade, msg, update_gsheet=True)
                    return 0
            if leg.action == 'BUY':
                price += _getprice(p)
            else:
                price -= _getprice(p)
    else:
        p = get_price_by_contract_id(session, trade.contract_id, min_seconds=60)
        if p is None:
            sub, p = register_market_data_subscription(session, trade.gsheet_trade.get_contract(), ib_app)
            if p is None:
                if raise_gsheet_error:
                    msg = IbTradeMessage()
                    msg.error_code = 99992
                    msg.text = "Failure to retrieve pricing data within 30 seconds. " \
                               "Try again (maybe fix contract details)."
                    register_trade_msg(trade, msg, update_gsheet=True)
                return 0
        price = _getprice(p)
    return price


def maybe_request_executions(session, ib_app):
    orders = session.query(IbOrder).filter(IbOrder.status == OrderStatus.PLACED).all()
    for order in orders:
        execs = get_executions_by_order_id(session, order.request_id)
        if not execs:
            ib_app.request_executions()
            break


def maybe_update_db_trade(session, sql_trade: IbTrade, sheet_trade):
    """ Updates stop_price/target_prices"""
    update_needed = False
    checks = ('stop_price1',
              'stop_price2',
              'target_price1',
              'target_price2',
              'target_price3')

    for attr in checks:

        sql_attr = getattr(sql_trade, attr, 0)
        g_attr = getattr(sheet_trade, attr, 0)

        if sql_attr != g_attr:
            update_needed = True
            break

    if not update_needed:
        return False

    for attr in checks:
        new_val = getattr(sheet_trade, attr, None)
        setattr(sql_trade, attr, new_val)

    session.commit()
    return sql_trade


def place_orders(session, ib_app):
    stocks_open = utils.get_market_stocks_open()
    options_open = utils.get_market_options_open()

    if not any((stocks_open, options_open)):
        return

    orders = session.query(IbOrder).filter(
        IbOrder.status == OrderStatus.READY
    ).all()

    if not orders:
        return

    for order in orders:
        market_open = stocks_open if order.contract.sec_type == 'STK' else options_open
        if not market_open:
            continue

        place_order(session, ib_app, order)


def place_order(session, ib_app, order: IbOrder, peg_mid=True):

    contract = order.contract.get_ib_contract(order.trade_id)
    combo_legs = getattr(contract, 'comboLegs', None)

    if combo_legs:
        con_ids = [c.conId for c in combo_legs if c.conId]
        if len(con_ids) != len(combo_legs):
            return False

    if not order.request_id:
        order.request_id = ib_app.next_id()

    order.status = OrderStatus.PLACED
    ib_order = order.ib_order

    if not utils.get_market_options_open():
        ib_order.outsideRth = True

    log.debug("Placing order: {} / Contract: {}".format(order, contract))
    ib_app.placeOrder(order.request_id, contract, ib_order)
    session.commit()


_THREADS = dict()


def process_pegged_order(ib_app, order, timeout=90):
    thread = _THREADS.get(order.request_id, None)
    if thread is None:
        _THREADS[order.request_id] = thread = Thread(
            target=_process_threaded_peg_order,
            args=(Session, ib_app, order.request_id, timeout))
        thread.start()
    else:
        try:
            thread.join(0.05)
        except TimeoutError:
            return
        else:
            _THREADS.pop(order.request_id)


def _process_threaded_peg_order(Session, ib_app, order_id, timeout=90):
    session = Session()
    order = session.query(IbOrder).filter(IbOrder.request_id == order_id).one()

    try:
        done = _process_pegged_order(session, ib_app, order, timeout)
    except Exception as e:
        log.error(e)
        session.rollback()
    else:
        session.commit()
        session.close()
        return done


def _process_pegged_order(session, ib_app, order: IbOrder, timeout=90):
    timeout_time = datetime.now() + timedelta(seconds=timeout)
    session.commit()
    log.debug("process_pegged_order: {} - need {}".format(order.contract_id, order.qty))
    while timeout_time > datetime.now():
        if not utils.get_market_options_open():
            return False
        if order.status != OrderStatus.PLACED:
            return False

        ib_app.request_executions()
        # We want a filled order.
        execs = order.get_valid_executions(get_executions_by_order_id(session, order.request_id))
        filled = order.get_executed_qty(execs)

        if filled >= order.qty:
            return True

        mid = get_trade_market_price(session, order.trade, ib_app, timeout_seconds=10, raise_gsheet_error=False)

        if mid is None:
            log.debug("process_pegged_order: {} - missing pricing.".format(order.contract_id))
            sleep(2.5)
            continue

        # Maybe replace order to mid.
        needs_replacement = abs(abs(order.price) - abs(mid)) > STK_NBBO_OFFSET

        if needs_replacement:
            log.debug("process_pegged_order: Replacing price {} with {}".format(order.price, mid))
            order.price = mid
            session.commit()
            place_order(session, ib_app, order)
        session.expire(order)
        sleep(5)

    if order.trade:
        execs = get_executions_by_order_id(session, order.request_id)
        if not execs and not order.trade.has_opening_order_complete:
            msg = IbTradeMessage()
            msg.status = MsgStatus.OPEN
            msg.error_code = 99991
            msg.text = "Order peg-to-mid timeout - reset trade & try again."
            register_trade_msg(order.trade, msg, update_gsheet=True)
            register_forced_trade_close(session, order.trade, exclude=1)
            order.status = OrderStatus.ERROR
            ib_app.cancelOrder(order.request_id)
        else:
            # Chase the partial or closing trade trade until complete.
            log.debug("Retrying order: {}".format(order))
            return _process_pegged_order(session, ib_app, order,)

    return False


def process_trade_messages(session, ib_app):
    msgs = session.query(IbTradeMessage).filter(
        and_(IbTradeMessage.status == MsgStatus.OPEN,
             IbTradeMessage.error_code > 0)
    ).join(IbTrade).filter(
        IbTrade.status.notin_([TradeStatus.CLOSED, TradeStatus.ERROR])
    ).all()

    if not msgs:
        return

    log.debug("process_trade_messages: {} messages".format(len(msgs)))
    highlight_codes = ib.CODES_USER_ERROR + ib.CODES_PROGRAMMING_ERROR

    for msg in msgs:

        trade = msg.trade
        code = msg.error_code
        msg.status = MsgStatus.UNKNOWN
        highlight_trade = False
        send_email = False

        if code in ib.CODES_IGNORE:
            msg.status = MsgStatus.RESOLVED
            msg.date_resolved = datetime.utcnow()
            continue

        # Cancelled Order Message
        elif code == 202 and USE_LMT_ORDERS and LMT_TIME_IN_FORCE == 'FOK':
            if trade.has_opening_order:
                # A closing order failed.
                total_202s = len([m for m in trade.messages if m.error_code == 202])
                if total_202s >= 3:
                    # Don't replace order - something is wrong.
                    trade.status = TradeStatus.ERROR
                    highlight_trade = True
                    send_email = True
                else:
                    # Replace order using the original order.
                    orig_order = None
                    for o in trade.orders:
                        if o.request_id == msg.request_id:
                            orig_order = o
                            break

                    if orig_order is not None:
                        price = get_trade_limit_price(
                            session, trade, orig_order.action, ib_app)
                        register_order(session, trade, orig_order.action,
                                       orig_order.qty, tif=orig_order.tif,
                                       price=price, type=orig_order.type)
                    else:
                        log.error("process_trade_messages: Expected to find order with request id {}".format(msg.request_id))
                        send_email = True
            else:
                # An opening order failed.
                trade.status = TradeStatus.ERROR
                highlight_trade = True

        # Highlight Code
        elif code in highlight_codes or 3 < msg.count < 5:
            trade.status = TradeStatus.ERROR
            highlight_trade = True

            if code in ib.CODES_PROGRAMMING_ERROR:
                send_email = True

        # HotFix Needed
        elif code in ib.CODES_IB_INTERNAL:
            send_email = True

        else:
            continue

        if highlight_trade:
            row = ibtrade.get_sheet_row_by_uid(trade.u_id)
            highlighted = ibtrade.highlight_cell(
                u_id=None,
                col_number=4,  # Tactic S or O
                row_idx=row,
                bg_color='red')
            if not highlighted:
                log.error("Failed to highlight UID {}".format(trade.u_id))

        if send_email:
            utils.send_notification("IB Error", msg.__repr__(), 'zekebarge@gmail.com')

        msg.status = MsgStatus.RESOLVED
        msg.date_resolved = datetime.utcnow()

    session.commit()


def register_market_data_subscription(session, api_contract, ib_app):
    """Registers a market data subscription with the database and interactive brokers if needed."""
    contract_id = api_contract.key
    sub = session.query(IbMktDataSubscription).filter(IbMktDataSubscription.contract_id == contract_id).one_or_none()

    if not sub:
        sub = IbMktDataSubscription()
        sub.contract_id = contract_id
        session.add(sub)

    price = get_price_by_contract_id(session, contract_id, min_seconds=60*5)
    if not price:
        if not sub.date_requested or sub.date_requested > datetime.utcnow() - timedelta(minutes=3):
            sub.request_subscription(ib_app, api_contract)

        session.commit()
        timeout = datetime.now() + timedelta(seconds=60)
        while datetime.now() < timeout:
            price = get_price_by_contract_id(session, contract_id, min_seconds=60*5)
            if price:
                break
            sleep(3)

        if not price:
            # fail the trade w/ message + send error.
            return sub, None
    return sub, price


def register_contract(session, contract: ibutils.Contract, trade: IbTrade = None, parent_id: int=None):
    """Registers new IbContracts, and IbTradeLegs (if associated). """
    c = session.query(IbContract).filter(IbContract.contract_id == contract.key).one_or_none()

    if c is None:
        c = IbContract()
        c.contract_id = contract.key
        c.sec_type = contract.secType
        c.symbol = contract.symbol
        c.exchange = contract.exchange
        c.expiration = contract.lastTradeDateOrContractMonth
        c.strike = contract.strike
        c.right = contract.right
        c.parent_contract_id = parent_id
        session.add(c)
        log.debug("register_contract: new {}".format(c))

    elif not c.ib_contract_id and contract.conId:
        c.ib_contract_id = contract.conId

    if trade is not None:
        if trade.sec_type == 'BAG':
            sheet_trade = trade.gsheet_trade
            sheet_trade.get_contract()
            for i, leg in enumerate(sheet_trade.leg_data, start=1):
                _register_trade_leg(session, trade, leg, i)
        session.commit()
        session.expire(trade)
        api_contract = c.get_ib_contract(trade.id)
        if trade.contract_id == api_contract.key and not trade.contract_pk:
            trade.contract_pk = c.id
        elif trade.underlying_contract_id == api_contract.key and not trade.underlying_contract_pk:
            trade.underlying_contract_pk = c.id

    session.commit()

    return c


def register_contract_details(session, contract, details):
    contracts = session.query(IbContract).filter(IbContract.contract_id == contract.key).all()
    detail = details[-1]
    for contract in contracts:
        contract.ib_contract_id = detail.underConId
    return contracts


def register_executions(session, executions):
    log.debug("register_executions")
    exec_ids = [e.execId for _, e in executions]
    qry = session.query(IbExecution).filter(IbExecution.exec_id.in_(exec_ids))
    matches = {e.exec_id: e for e in qry.all()}

    for contract, execution in executions:
        match = matches.get(execution.execId, None)
        if match:
            if not match.utc_time:
                match.utc_time = ibutils.get_utc_from_server_time(execution.time)
                session.commit()

        else:

            match = IbExecution()
            match.exec_id = execution.execId

            base_exec_id, correction_id = _split_exec_correction_id(execution.execId)
            match.base_exec_id = base_exec_id
            match.correction_id = correction_id
            match.utc_time = ibutils.get_utc_from_server_time(execution.time)
            match.order_id = execution.orderId
            match.server_time = execution.time
            match.acc_number = execution.acctNumber
            match.exchange = execution.exchange
            match.side = execution.side
            match.shares = execution.shares
            match.price = execution.price
            match.cum_qty = execution.cumQty
            match.avg_price = execution.avgPrice
            match.contract_id = contract.key
            match.ib_contract_id = contract.conId

            session.add(match)
            session.commit()
            log.debug("register_executions: new {}".format(match))
        matches[match.exec_id] = match
    return matches


def register_ib_contract_ids(session, _, details):
    for detail in details:
        cont = ibutils.Contract.from_ib(detail.contract)
        register_ib_contract_id(session, cont.key, cont.conId)


def register_ib_contract_id(session, contract_id, ib_contract_id):
    condition = and_(IbTradeLeg.contract_id == contract_id,
                     IbTradeLeg.ib_contract_id == None)
    legs = session.query(IbTradeLeg).filter(condition).all()
    for leg in legs:
        leg.ib_contract_id = ib_contract_id
    session.commit()


def register_ib_error(session, req_id, error_code, error_msg):
    order = session.query(IbOrder).filter(IbOrder.request_id == req_id).one_or_none()
    msg_text = error_msg + ' code({})'.format(error_code)

    if order:
        order.status = OrderStatus.ERROR
        trade = order.trade
        msg = IbTradeMessage()
        msg.error_code = error_code
        msg.request_id = req_id
        msg.text = msg.text_public = msg_text
        if trade is not None:
            register_trade_msg(trade, msg)
        else:
            session.add(msg)
            session.commit()
        return

    sub = session.query(IbMktDataSubscription).filter(
        IbMktDataSubscription.request_id == req_id
    ).one_or_none()

    if not sub:
        return

    trades = session.query(IbTrade).filter(
        or_(IbTrade.contract_id == sub.contract_id,
            IbTrade.underlying_contract_id == sub.contract_id)
    ).all()
    for t in trades:
        msg = IbTradeMessage()
        msg.text = msg.text_public = msg_text
        msg.error_code = error_code
        msg.request_id = req_id
        register_trade_msg(t, msg)


def register_ib_price(session, contract, price_data):
    p = IbPrice()
    p.contract_id = contract.key
    p.bid = price_data['bid']
    p.ask = price_data['ask']
    p.price = price_data['mid']
    p.time = price_data['mid_time']
    try:
        session.add(p)
        session.commit()
    except (OperationalError, IntegrityError) as e:
        # log.error(e)
        session.rollback()


def register_mkt_data_activity(session, req_id):
    subscription = session.query(IbMktDataSubscription).filter(
        IbMktDataSubscription.request_id == req_id
    ).one_or_none()
    if subscription:
        subscription.active_date = datetime.utcnow()


def register_order(session, trade, action, qty, request_id=None, 
                   exclude=0, status=OrderStatus.READY, tif=None, 
                   price=None, type='MKT', offset=None):
    o = IbOrder()
    o.u_id = trade.u_id
    o.action = action
    o.qty = qty
    o.status = status
    o.trade_id = trade.id
    o.contract_id = trade.contract_id
    o.exclude = exclude
    o.request_id = request_id
    o.tif = tif
    o.price = price
    o.type = type
    o.offset = offset
    session.add(o)
    session.commit()
    log.debug("register_order: new {}".format(o))
    return o


def register_positions(session, account_name, portfolio):
    """ callback for IbDbApp.accountDownloadEnd """
    matches = {p.contract_id: p for p in session.query(IbPosition).filter(
               IbPosition.account_name == account_name).all()}

    for contract_id, data in portfolio.items():
        try:
            match = matches[contract_id]
            match.position = data['position']
            match.market_price = data['market_price']
            match.time = datetime.utcnow()
            match.checked = 0
        except KeyError:
            if data['position'] == 0:
                continue
            match = IbPosition()
            match.symbol = data['symbol']
            match.security_type = data['security_type']
            match.position = data['position']
            match.market_price = data['market_price']
            match.account_name = data['account_name']
            match.contract_id = contract_id
            session.add(match)
        session.commit()


def register_position(session, account_name, contract, position):
    """ callback for IbDbApp.position """
    match = session.query(IbPosition).filter(
        and_(IbPosition.account_name == account_name,
             IbPosition.contract_id == contract.key)
    ).one_or_none()
    if match is None:
        if position != 0:
            match = IbPosition()
            match.contract_id = contract.key
            match.symbol = contract.symbol
            match.position = position
            match.account_name = account_name
            match.security_type = contract.secType
            session.add(match)
            log.debug("register_position: new {}".format(match))
    else:
        match.position = position
        match.valid = 1
        match.checked = 0
        match.time = datetime.utcnow()
    try:
        session.commit()
    except StaleDataError:
        session.rollback()
        return


def register_trade(session, trade):
    if hasattr(trade, '__iter__'):
        trade = Trade.from_gsheet_row(trade)
    return _register_trade_from_trade_obj(session, trade)


def register_trade_msg(trade, msg, update_gsheet=True):
    existing = [t for t in trade.messages
                if t.text == msg.text
                and t.status == MsgStatus.OPEN]
    if existing:
        existing[0].count += 1
        existing[0].date_last_occured = datetime.utcnow()
    else:
        trade.messages.append(msg)
        if update_gsheet:
            ibtrade.log_trade_error(
                trade.symbol,
                msg.text,
                trade.u_id,
                error_code=msg.error_code)
    log.debug("register_trade_msg: {}".format(msg))


def request_ib_contract_ids(session, ib_app):
    """Requests IbTradeLeg.ib_contract_id from ibapi. This makes BAG trades possible."""
    legs = session.query(IbTradeLeg).filter(
        and_(IbTradeLeg.ib_contract_id == None,
             IbTradeLeg.registration_attempts <= 3)).all()

    for leg in legs:
        contract = leg.get_request_contract()
        ib_app.request_contract_id(contract)
        leg.date_requested = datetime.utcnow()
        leg.registration_attempts += 1

    session.commit()


def sync_expired_trades(session, force=False):
    if not force:
        now = utils.now_est()
        if now.hour != 12 or now.minute > 5:
            return

    contract_ids = [i for i in get_open_contract_ids(session) if '-' in i]
    if not contract_ids:
        return

    contracts = session.query(IbContract).filter(IbContract.contract_id.in_(contract_ids)).all()
    today = datetime.now().date()
    expired_contracts = list()

    for c in contracts:
        expiry = datetime.strptime(c.expiration, '%Y%m%d').date()
        if expiry == today:
            expired_contracts.append(c)

    if not expired_contracts:
        return

    expired_ids = [c.contract_id for c in expired_contracts]

    bag_ids = session.query(IbTradeLeg.trade_id).filter(IbTradeLeg.contract_id.in_(expired_ids)).all()
    bag_ids = [b.trade_id for b in bag_ids]

    recent_orders = session.query(IbOrder.trade_id).filter(IbOrder.date_added > datetime.utcnow() - timedelta(minutes=10)).all()
    recent_ids = [o.trade_id for o in recent_orders]

    trades = session.query(IbTrade).filter(
        and_(or_(IbTrade.id.in_(bag_ids),
                 IbTrade.contract_id.in_(expired_ids)),
             IbTrade.status == TradeStatus.OPEN,
             IbTrade.id.notin_(recent_ids))
    ).all()

    for trade in trades:
        qty = trade.left_qty
        action = 'BUY' if trade.is_short else 'SELL'
        register_order(session, trade, action, qty)


def sync_gsheet_trades(session):
    """Updates the database with trades from the GSheet."""
    #ibtrade.log.debug("Syncing gsheet_trades.")
    rows = ibtrade.get_data_entry_rows()
    trades = get_data_entry_trades(rows=rows)

    for trade in trades:
        register_trade(session, trade)

    trades_closed = ibtrade.get_data_entry_trades_closed(
        rows=rows)
    if trades_closed:
        sync_gsheet_manually_closed_trades(session, trades_closed)


def sync_gsheet_manually_closed_trades(session, trades):
    pending_orders = session.query(IbOrder.u_id).filter(
        and_(IbOrder.status.in_(OrderStatus.PENDING_STATUSES))).all()
    pending_u_ids = [o.u_id for o in pending_orders]
    u_ids = [str(t.u_id) for t in trades
             if str(t.u_id) not in pending_u_ids]

    matches = session.query(IbTrade).filter(
        and_(
            IbTrade.u_id.in_(u_ids),
            IbTrade.u_id.notin_(pending_u_ids),
            IbTrade.status == TradeStatus.OPEN,
            IbTrade.date_exited.is_(None))).all()
    matches = {t.u_id: t for t in matches}

    if not matches:
        return

    for t in trades:
        db_trade = matches.get(str(t.u_id), None)

        if not db_trade:
            continue

        if db_trade.is_long:
            action = 'SELL'
            qty = db_trade.total_qty - db_trade.sold_qty
        else:
            action = 'BUY'
            qty = db_trade.total_qty - db_trade.bought_qty

        register_order(session, db_trade, action, qty)


def sync_opening_orders(session, ib_app):
    open_orders = session.query(IbOrder.trade_id).filter(
        and_(IbOrder.exclude == 0,
             IbOrder.date_added > datetime.utcnow() - timedelta(days=5))).all()
    trade_ids = [o.trade_id for o in open_orders]
    trades = session.query(IbTrade).filter(
        and_(IbTrade.date_exited.is_(None),
             IbTrade.status == TradeStatus.OPEN,
             IbTrade.u_id.isnot(None),
             IbTrade.underlying_contract_id.isnot(None),
             IbTrade.id.notin_(trade_ids),
             and_(IbTrade.entry_price.is_(None),
                  IbTrade.original_entry_price.isnot(None)),
             IbTrade.size.isnot(None))).all()

    for t in trades:
        if not t.has_valid_legs:
            continue
        action = 'SELL' if t.is_short else 'BUY'
        contract_price = None
        tif = None
        offset = None

        if USE_LMT_ORDERS:
            order_type = 'LMT'
            if 'STK' == t.sec_type:
                tif = 'PEG MID'
                offset = STK_NBBO_OFFSET
            else:
                contract_price = get_trade_limit_price(session, t, action, ib_app)
        else:
            order_type = 'MKT'

        register_order(session, t, action, t.total_qty,
                       status=OrderStatus.READY,
                       tif=tif,
                       price=contract_price,
                       type=order_type,
                       offset=offset)


def sync_positions(session):
    if not SYNC_POSITIONS:
        return
    if not utils.get_market_options_open():
        return

    positions = session.query(IbPosition).filter(
        and_(
             IbPosition.time > datetime.utcnow() - timedelta(minutes=10),
             IbPosition.position != 0,
             IbPosition.valid == 1,
             IbPosition.checked == 0)).all()

    # Avoid checking trades with recently opened orders.
    recent_orders = session.query(IbOrder.trade_id).filter(
        IbOrder.date_added > datetime.utcnow() - timedelta(minutes=5)
    ).all()
    recent_order_trade_ids = [o.trade_id for o in recent_orders]

    for pos in positions:
        pos.checked = 1
        trades = session.query(IbTrade).filter(
            and_(IbTrade.contract_id == pos.contract_id,
                 IbTrade.status == TradeStatus.OPEN,
                 IbTrade.date_exited.is_(None),
                 IbTrade.entry_price > 0)).all()

        if not trades:
            trades = session.query(IbTrade, IbTradeLeg).filter(
                IbTrade.status == TradeStatus.OPEN
            ).join(IbTradeLeg).filter(
                IbTradeLeg.contract_id == pos.contract_id
            ).all()
            if not trades:
                _register_orphan_position(session, pos)
            else:
                # Avoid monitoring multi-leg contracts.
                continue

        for t in trades:
            if t.id in recent_order_trade_ids:
                # Give time for orders to complete.
                continue

            total_qty = t.total_qty
            action = None
            qty = None

            if t.is_short:
                if t.sold_qty > total_qty:
                    action = 'BUY'
                    qty = t.sold_qty - total_qty
                elif t.bought_qty > total_qty:
                    action = 'SELL'
                    qty = t.bought_qty - total_qty
            else:
                if t.bought_qty > total_qty:
                    action = 'SELL'
                    qty = t.bought_qty - total_qty
                elif t.sold_qty > total_qty:
                    action = 'BUY'
                    qty = t.sold_qty - total_qty

            if action:
                t.status = TradeStatus.ERROR
                msg_text = "programming error: {} position size is off by {}".format(t.contract_id, qty)
                msg = IbTradeMessage()
                msg.error_code = 99994
                msg.text = msg_text
                register_trade_msg(t, msg, update_gsheet=True)

                if TEST_MODE or ibtrade.SHEET_TEST_MODE:
                    log.error(msg_text)
                    #register_order(session, t, action, qty, exclude=1)
                else:
                    log.error(msg_text)

    session.commit()


def sync_pre_check_trades(session, ib_app):
    """
    Moves an IbTrade.status from PRE_OPEN_CHECK to OPEN as long as:

    1) A price can be found for the trade contract(s)
        - Registers a 99992 error otherwise.
    2) The original_entry_price (if not null) is within 5% of the market price.
        - Registers a 9993 error otherwise.
    Marks TradeStatus.CLOSED when trade.date_exited exists.
    """
    trades = session.query(IbTrade).filter(IbTrade.status == TradeStatus.PRE_OPEN_CHECK).all()
    if not trades:
        return

    for trade in trades:
        if trade.date_exited:
            trade.status = TradeStatus.CLOSED
            continue

        og_entry = trade.original_entry_price
        if not og_entry:
            # Nothing to pre-check, release trade.
            trade.status = TradeStatus.OPEN
            continue

        # Need valid trade legs.
        if not trade.has_valid_legs:
            if trade.date_added < datetime.utcnow() - timedelta(seconds=30):
                msg = IbTradeMessage()
                msg.error_code = 99995
                msg.text = "Missing contract leg(s) - likely invalid TACTIC. Trade reset needed."
                register_trade_msg(trade, msg, update_gsheet=True)
            continue

        # Get contract price
        price = get_trade_market_price(session, trade, ib_app, raise_gsheet_error=True)
        if not price:
            continue

        # Open trade (status) if price within 5% (sanity check)
        diff = abs(abs(og_entry) - abs(price))
        if abs(diff) > 0.05 * abs(price):
            msg = IbTradeMessage()
            msg.error_code = 99993
            msg.text = "original entry price ({}) " \
                       "is more than 5% away from market price ({})".format(og_entry, price)
            register_trade_msg(trade, msg, update_gsheet=True)
            continue
        else:
            trade.status = TradeStatus.OPEN

        # Maybe update size (db & gsheet)
        check_size = ibutils.get_corrected_sheet_size(
            trade.size, price,
            split=len(trade.target_prices),
            size_factor=POSITION_SIZE_FACTOR)

        if trade.sec_type == 'BAG':
            check_size = abs(check_size)

        if check_size != trade.size:
            log.debug(
                "sync_pre_check_trades: Update size from {} to {} on {}".format(trade.size, check_size, trade))
            trade.size = check_size
            ibtrade.set_new_trade_size(trade.u_id, check_size)

    session.commit()


def sync_price_subscriptions(session, ib_app):
    recent_prices = session.query(IbPrice).filter(
        IbPrice.time > datetime.utcnow() - timedelta(minutes=10)).all()
    recent_contract_ids = list(set([p.contract_id for p in recent_prices]))
    open_contract_ids = get_open_contract_ids(session)

    missing_contract_ids = [c for c in open_contract_ids if c not in recent_contract_ids]
    unneeded_contract_ids = [c for c in recent_contract_ids if c not in open_contract_ids]
    subs = session.query(IbMktDataSubscription)

    # Add missing subscriptions
    for id in missing_contract_ids:
        sub = subs.filter(IbMktDataSubscription.contract_id == id).one_or_none()
        if sub is None:
            sub = IbMktDataSubscription()
            sub.contract_id = id
            sub.date_requested = datetime.utcnow()
            session.add(sub)
        elif sub.date_requested and sub.date_requested > datetime.utcnow() - timedelta(minutes=5):
            continue

        contract = get_api_contract_by_contract_id(session, id)
        if contract is not None:
            sub.refresh_subscription(ib_app, contract)

    # Remove unneeded subscriptions
    if unneeded_contract_ids:
        unneeded_subs = subs.filter(
            and_(IbMktDataSubscription.contract_id.in_(unneeded_contract_ids),
                 IbMktDataSubscription.request_id.isnot(None))
        ).all()

        for sub in unneeded_subs:
            sub.cancel_subscription(ib_app)


def sync_timed_out_orders(session):
    mins_to_mkt = utils.get_minutes_to_market_open()
    if mins_to_mkt > 0:
        return  # Don't process outside of market hours.
    elif 0 > mins_to_mkt > -30:
        return   # Allow ~30 mins of market time to fill open orders.

    # TODO: We have the opportunity to replace timed out orders
    # Since we're identifying them here. Make a process for this.
    # When order.type == 'LMT': Cancel/replace @ new current price.
    orders = session.query(IbOrder).filter(
        and_(IbOrder.status.in_(OrderStatus.PENDING_STATUSES),
             IbOrder.date_added < datetime.utcnow() - timedelta(minutes=15))
    ).all()

    for order in orders:
        order.status = OrderStatus.ERROR
        trade = order.trade

        # Avoid timing out forced & exit trades.
        if trade is None or trade.has_opening_order_complete:
            continue

        msg = IbTradeMessage()
        msg.error_code = 99991
        msg.text = "Order failed to execute (or register execution) within 15 minutes."
        msg.status = MsgStatus.OPEN
        register_trade_msg(trade, msg, update_gsheet=True)

        # TODO: Close partial(s) if any.
        session.commit()


def sync_fills(session):
    """Processes IbOrders from PLACED to COMPLETE, updating GSheet/IbTrade with details from IbExecutions."""
    orders = session.query(IbOrder).filter(IbOrder.status == OrderStatus.PLACED).all()
    for order in orders:
        log.debug("sync_fills: {}".format(order))

        all_execs = get_executions_by_order_id(session, order.request_id)
        execs = order.get_valid_executions(all_execs)
        qty = order.get_executed_qty(execs)
        # log.debug("# of executions: {}".format(len(execs)))
        # log.debug("exec qty: {}".format(qty))
        if abs(qty) < abs(order.qty):
            log.debug("Skipping partially-filled order.".format(order.contract_id, len(execs), qty))
            continue

        price = order.get_executed_price(execs)
        db_trade = order.trade
        now_utc = max(execs, key=lambda e: e.utc_time).utc_time

        log.debug("exec price: {} @ {}".format(price, db_trade))
        position = session.query(IbPosition).filter(IbPosition.contract_id == order.contract_id).one_or_none()
        if position and abs(position.position) >= order.qty:
            if order.action == 'BUY':
                position.position += order.qty
            else:
                position.position -= order.qty

        if db_trade is None:
            order.status = OrderStatus.COMPLETE
            order.date_filled = now_utc
            continue

        now_est = utils.utc_to_est(now_utc).strftime(ibtrade.SHEET_TIME_FMT)

        attrs = {'date_entered': None, 'date_exited': None,
                 'entry_price': None,  'exit_price': None,
                 'is_partial': None,   'pct_sold': None, 'notes': None}

        # Compose trade attributes.
        if db_trade.is_long:
            if order.action == 'BUY':
                attrs['date_entered'] = now_est
                attrs['entry_price'] = price
            else:
                pct_sold = (abs(qty)/abs(db_trade.total_qty))*100
                total_sold = db_trade.sold_qty + abs(qty)
                attrs['pct_sold'] = pct_sold
                attrs['is_partial'] = False if total_sold >= db_trade.total_qty else True
                attrs['date_exited'] = now_est
                attrs['exit_price'] = price
                attrs['notes'] = ibtrade.TGT_REACHED if price > db_trade.entry_price else ibtrade.STOP_LOSS
                if db_trade.sec_type == 'BAG':
                    if db_trade.entry_price < 0:
                        # credit entry / debit exit
                        if abs(price) > abs(db_trade.entry_price):
                            attrs['notes'] = ibtrade.STOP_LOSS
                        else:
                            attrs['notes'] = ibtrade.TGT_REACHED

            # Assumes all BAG trades are "LONG" to IB. Even when the sub-contracts make the position
            # A technical short trade. (e.g credit trades)
            if db_trade.sec_type == 'BAG':
                key = 'entry_price' if order.action == 'BUY' else 'exit_price'
                if db_trade.original_entry_price and db_trade.original_entry_price < 0:
                    # Credit BAG
                    attrs[key] = -abs(attrs[key])
                else:
                    # Debit
                    attrs[key] = abs(attrs[key])

        else:
            if order.action == 'SELL':
                attrs['date_entered'] = now_est
                attrs['entry_price'] = -price
            else:
                pct_sold = (abs(qty) / abs(db_trade.total_qty)) * 100
                total_sold = db_trade.bought_qty + abs(qty)
                attrs['pct_sold'] = pct_sold
                attrs['is_partial'] = False if total_sold >= db_trade.total_qty else True
                attrs['date_exited'] = now_est
                attrs['exit_price'] = -price
                attrs['notes'] = ibtrade.TGT_REACHED if price < db_trade.entry_price else ibtrade.STOP_LOSS
        log.info("closing attributes: {}".format(attrs))

        # Update GSheet
        if attrs['is_partial']:
            log.debug("Closing partial")
            sheet_trade_partial = ibtrade.close_sheet_trade_partial(
                db_trade.u_id, attrs['pct_sold'], attrs['exit_price'],
                attrs['date_exited'], attrs['notes'])
            db_trade_partial = register_trade(session, sheet_trade_partial)
            db_trade_partial.parent_trade_id = db_trade.id
            db_trade_partial.status = TradeStatus.CLOSED
        else:

            if attrs['date_exited']:
                log.debug("Closing trade.")
                u_id = ibtrade.close_sheet_trade(
                    db_trade.u_id, attrs['pct_sold'], attrs['exit_price'],
                    attrs['date_exited'], attrs['notes'])
                db_trade.u_id = str(u_id)
                db_trade.exit_price = abs(attrs['exit_price'])
                db_trade.date_exited = now_utc
                db_trade.status = TradeStatus.CLOSED
            else:
                log.debug("Opening trade.")
                ibtrade.open_sheet_trade(
                    db_trade.u_id,
                    attrs['entry_price'],
                    attrs['date_entered'])
                db_trade.entry_price = abs(attrs['entry_price'])
                db_trade.date_entered = now_utc

        order.status = OrderStatus.COMPLETE
        order.date_filled = now_utc
    session.commit()


def sync_invalid_trade_contracts(session):
    """Highlights GSheet TACTIC red when a pricing error occurs."""
    text = "Contract id failed to receive pricing from IB 3 times."
    done_results = session.query(IbTradeMessage.trade_id).filter(
        and_(IbTradeMessage.text == text,
             IbTradeMessage.date_added > datetime.utcnow() - timedelta(days=5))).all()
    done_ids = [r.trade_id for r in done_results]
    new_trades = session.query(IbTrade).filter(
        and_(IbTrade.date_exited == None,
             IbTrade.registration_attempts > 3,
             IbTrade.id.notin_(done_ids))).all()

    handled_ids = list()
    for trade in new_trades:
        ibtrade.highlight_cell(trade.u_id, 4, bg_color='red')

        msg = IbTradeMessage()
        msg.text = text
        msg.text_public = "Untracked trade."
        msg.status = 'open'

        register_trade_msg(trade, msg)
        handled_ids.append(trade.id)

    old_trades = session.query(IbTrade, IbTradeMessage).filter(
        and_(IbTrade.date_exited == None,
             IbTrade.registration_attempts > 3,
             IbTrade.id.notin_(handled_ids)
             )).join(IbTradeMessage).filter(
        and_(IbTrade.date_updated > IbTradeMessage.date_added,
             IbTradeMessage.status == 'open',
             IbTradeMessage.text == text)).all()

    for trade, msg in old_trades:
        ibtrade.highlight_cell(trade.u_id, 4, bg_color='white')

        trade.registration_attempts = 0
        trade.date_updated = datetime.utcnow()

        msg.status = 'closed'
        msg.date_resolved = datetime.utcnow()

    if new_trades or old_trades:
        session.commit()


def sync_peg_limit_orders(session, ib_app):
    orders = session.query(IbOrder).filter(
        and_(IbOrder.status == OrderStatus.PLACED,
             IbOrder.type == 'LMT',
             IbOrder.tif.is_(None))
    ).all()

    for o in orders:
        process_pegged_order(ib_app, o, timeout=90)


def _register_orphan_position(session, pos: IbPosition):
    pos.valid = 0
    contracts = session.query(IbContract).filter(
        IbContract.contract_id == pos.contract_id
    ).all()

    contract = None
    if not contracts:
        contract = pos.get_guessed_ib_contract()
        if contract is None:
            return
        register_contract(session, contract)
    else:
        try:
            contract = contracts[0].ib_contract
        except AttributeError:
            return

    if contract:
        size = pos.position
        order = IbOrder()
        order.contract_id = pos.contract_id
        order.exclude = 1
        order.qty = abs(size)
        order.action = 'SELL' if size > 0 else 'BUY'
        order.status = OrderStatus.READY
        session.add(order)

    session.commit()

    return order


def _same_val(x, y):
    if not x and not y:
        return True
    for dtype in (float, bool, str):
        try:
            return dtype(x) == dtype(y)
        except:
            pass


def _get_price_data_import(contract: ibutils.Contract, data: dict):
    """Calculates mid, mid_time prices. If data is returned it should be uploaded to ib_prices."""
    throttle_key = 'handle_price' + contract.key

    if CACHE_1_SEC.get(throttle_key, None):
        return

    bid = data.get('bid', None)
    bid_time = data.get('bid_time', None)
    ask = data.get('ask', None)
    ask_time = data.get('ask_time', None)

    if not all((bid, ask)):
        return

    check_time = datetime.utcnow() - timedelta(seconds=30)
    if bid_time < check_time or ask_time < check_time:
        return

    CACHE_1_SEC[throttle_key] = True
    last_mid = MAP_10_SEC.get(throttle_key, None)
    MAP_10_SEC[throttle_key] = mid_time = min(bid_time, ask_time)

    if mid_time == last_mid:
        mid_time = datetime.utcnow()

    mid = (bid+ask)/2
    if contract.secType in ('STK', 'OPT', 'BAG'):
        mid = round(mid, 2)

    data['mid'] = mid
    data['mid_time'] = mid_time

    return data


def _register_trade_leg(session, trade: IbTrade, leg: dict, sequence: int):
    match = [t for t in trade.legs
             if str(t.ratio) == str(leg['ratio'])
             and str(t.expiration) == str(leg['expiration'])
             and str(t.contract_id) == str(leg['contract'].key)]
    if match:
        return

    o = IbTradeLeg()
    o.ratio = leg['ratio']
    o.action = leg['action']
    o.expiration = '{}{}{}'.format(leg['year'], leg['month'], leg['day'])
    o.symbol = leg['symbol']
    o.strike = leg['strike']
    o.sequence = sequence
    o.contract_id = leg['contract'].key
    trade.legs.append(o)

    log.debug("_register_trade_leg: new {}".format(o))

    req_contract = o.get_request_contract()
    assoc_contract = register_contract(session, req_contract)
    session.expire(assoc_contract)
    o.contract_pk = assoc_contract.id
    session.commit()


def _register_trade_from_trade_obj(session, trade: Trade) -> IbTrade:
    if not trade.tactic_parsed:
        trade.parse_tactic()

    if trade.u_id:
        check_id = utils.digits(trade.u_id)
        match = get_trade_by_uid(session, check_id)
        if match:
            match = maybe_update_db_trade(session, match, trade)
            return match

    t = IbTrade()

    if not trade.u_id:
        t.original_entry_price = trade.entry_price
        trade.init_new_trade()

    _set_sql_trade_from_gsheet_trade(t, trade)

    if trade.is_short:
        if t.size:
            t.size = -abs(t.size)
        if t.original_entry_price:
            t.original_entry_price = -abs(t.original_entry_price)

    if not trade.valid:
        t.status = TradeStatus.ERROR
        msg = IbTradeMessage()
        msg.text = "Error parsing TACTIC."
        msg.error_code = 99995
        t.messages.append(msg)
    else:
        t.status = TradeStatus.PRE_OPEN_CHECK

    session.add(t)
    session.commit()

    if t.status != TradeStatus.ERROR:
        contract1 = trade.get_contract()
        register_contract(session, contract1, trade=t)
        if contract1.secType != 'STK':
            contract2 = trade.get_stock_contract()
            register_contract(session, contract2, trade=t)
    log.debug("_register_trade_from_trade_obj: new {}".format(trade))
    return t


def _reset_trade(session, sql_trade: IbTrade, sheet_trade: Trade):
    """Force resets an IbTrade. The order is cancelled and it is treated as a new trade."""
    log.debug("_reset_trade: {}".format(sql_trade))
    register_forced_trade_close(session, sql_trade)
    confirmed_row = ibtrade.get_sheet_row_by_uid(sheet_trade.u_id)
    sheet_trade.row_idx = confirmed_row
    sheet_trade.u_id = None
    session.commit()
    return _register_trade_from_trade_obj(session, sheet_trade)


def register_forced_trade_close(session, trade: IbTrade, exclude=1):
    """
    Emergency method fully closes a trade by placing a market order
    with quantity = total # of shares executed (ib_executions table).
    BAG trades have their legs individually closed.
    :param session:
    :param trade:
    :param exclude:
    :return:
    """
    qtys = defaultdict(lambda x: 0)

    for order in trade.orders:
        for exec in order.get_valid_executions(get_executions_by_order_id(session, order.request_id)):
            if exec.side == 'BOT':
                qtys[exec.contract_id] += exec.shares
            else:
                qtys[exec.contract_id] -= exec.shares

    for contract_id, qty in qtys.items():
        if qty == 0:
            continue
        elif qty < 0:
            qty = abs(qty)
            action = 'BUY'
        else:
            action = 'SELL'

        o = IbOrder()
        o.action = action
        o.qty = qty
        o.exclude = exclude
        o.status = OrderStatus.READY
        o.contract_id = contract_id
        session.add(o)

    session.commit()


def _set_sql_trade_from_gsheet_trade(sql_trade: IbTrade, sheet_trade: Trade):
    t, trade = sql_trade, sheet_trade

    t.alert_category = trade.alert_category
    t.date_entered = utils.est_to_utc(trade.date_entered)
    t.date_exited = utils.est_to_utc(trade.date_exited)
    t.exchange = trade.exchange
    t.pct_sold = utils.ensure_int_from_pct(trade.pct_sold)
    t.sec_type = trade.sec_type
    t.entry_price = utils.ensure_price(trade.entry_price)
    t.exit_price = utils.ensure_price(trade.exit_price)
    t.expiry_year = trade.expiry_year
    t.expiry_month = trade.expiry_month
    t.expiry_day = trade.expiry_day
    t.strike = trade.strike

    t.size = trade.size
    t.stop_price = str(trade.stop_price)
    t.stop_price1 = trade.stop_price1
    t.stop_price2 = trade.stop_price2
    t.symbol = trade.symbol
    t.tactic = trade.tactic
    t.target_price = str(trade.target_price)
    t.target_price1 = trade.target_price1
    t.target_price2 = trade.target_price2
    t.target_price3 = trade.target_price3
    t.u_id = utils.digits(trade.u_id)
    t.underlying_entry_price = trade.underlying_entry_price
    t.underlying_contract_id = getattr(sheet_trade.get_stock_contract(), 'key', None)
    t.contract_id = getattr(sheet_trade.get_contract(), 'key', None)


def _split_exec_correction_id(exec_id) -> (str, (None, int)):
    parts = exec_id.split('.')
    if len(parts) == 1:
        return parts[0], None
    else:
        exec_id = '.'.join(parts[:-1])
        c_id = parts[-1]
        if c_id.startswith('0'):
            c_id = int(c_id[1:])
        else:
            c_id = int(c_id)

        return exec_id, c_id


class IbDbApp(IbApp):

    @iswrapper
    def nextValidId(self, orderId: int):
        self.nextValidOrderId = orderId
        if self.subscribe:
            self.reqAccountUpdates(True, self.account_id)
            self.request_executions()

    def get_mid_by_contract_id(self, id):
        req_id = self._reqs_by_contract_key.get(id, None)
        if req_id is None:
            return None
        data = self.prices[req_id]
        return data.get('mid', None)

    @iswrapper
    def tickPrice(self, req_id, tick_type, price, attrib):
        """Track prices in the database (every few seconds)"""
        tick_type = tick_type_map.get(tick_type, None)
        if not tick_type:
            return

        now = datetime.utcnow()
        contract = self._contracts_by_req[req_id]
        data = self.prices[req_id]
        data[tick_type] = float(price)
        data[tick_type + '_time'] = now

        print("{}: {} {}: {}".format(now.strftime('%Y%m%d %H:%M:%S'), contract.key, tick_type, price))
        price = _get_price_data_import(contract, data)
        if price:
            call_with_session(register_ib_price, contract, price)
            call_with_session(register_mkt_data_activity, req_id)

    @iswrapper
    def contractDetails(self, req_id, details):
        self._contract_details[req_id].append(details)

    @iswrapper
    def contractDetailsEnd(self, req_id):
        contract = self._contracts_by_req.pop(req_id)
        details = self._contract_details.pop(req_id)
        call_with_session(register_ib_contract_ids, contract.key, details)

    @iswrapper
    def execDetails(self, req_id: int, contract, execution):
        if contract.secType == 'BAG':
            # We get individual legs
            # and calculate off that.
            return
        contract = ibutils.Contract.from_ib(contract)
        self.executions[req_id].append((contract, execution))

    @iswrapper
    def execDetailsEnd(self, req_id: int):
        try:
            executions = self.executions.pop(req_id)
            if not executions:
                return
            call_with_session(register_executions, executions)
        except KeyError:
            pass
        self.reqPositions()

    @iswrapper
    def error(self, error_id, error_code, error_msg):
        if 'farm connection is OK' in error_msg:
            return
        if 'farm is connecting' in error_msg:
            return
        if error_code == 300:
            return

        data = {'type': 'error',
                'error_code': error_code,
                'error_msg': error_msg,
                'time': utils.now_string()}
        msg = "API ERROR: {time}: ({error_code}) {error_msg}".format(**data)
        print(msg)
        log.error(msg)
        call_with_session(register_ib_error, error_id, error_code, error_msg)

    @iswrapper
    def accountDownloadEnd(self, account_name):
        p = self.portfolio[account_name]
        call_with_session(register_positions, account_name, p)
        p.clear()

    @iswrapper
    def position(self, account: str, contract: ibutils.IBContract, position: float, avgCost: float):
        contract = ibutils.Contract.from_ib(contract)
        call_with_session(register_position, account, contract, position)


def run_ib_database(ib_app, Session):
    """Executes trade management ibdb functions on an interval."""
    waiting_test = False
    core_errors = 0
    session = Session()
    Base.metadata.create_all(bind=session.bind)
    delete_old_prices(session, minutes=1)
    for sub in session.query(IbMktDataSubscription).all():
        sub.request_id = None
        sub.date_requested = None
    session.commit()
    session.close()

    if CANCEL_PLACED_ORDERS_ON_START:
        ib_app.reqGlobalCancel()

    while True:
        open, close = utils.get_nyse_open_close()
        now = utils.utcnow_aware()

        if ibtrade.get_sheet_test_mode():
            if not waiting_test:
                log.debug("Evaluations paused during test mode.")
                waiting_test = True
            sleep(EVAL_INTERVAL*2)
            continue
        else:
            waiting_test = False

        if now < open:
            seconds = (open - now).total_seconds()
            msg = "Waiting {} minutes for market open.".format(int(seconds / 60))

            log.debug(msg)
            print(msg)
            sleep(seconds)
            continue

        elif now > close + timedelta(minutes=30):
            log.debug("Market closed. Shutting down.")
            break

        session = Session()
        only_session = (session, )
        session_and_ib = (session, ib_app)

        funcs = [
            (sync_gsheet_trades,          only_session),
            (request_ib_contract_ids,   session_and_ib),
            (sync_price_subscriptions,  session_and_ib),
            (sync_pre_check_trades,     session_and_ib),
            (sync_peg_limit_orders,     session_and_ib),
            (sync_opening_orders,       session_and_ib),
            (evaluate_trades,           session_and_ib),
            (place_orders,              session_and_ib),
            (maybe_request_executions,  session_and_ib),
            (sync_fills,                  only_session),
            (process_trade_messages,    session_and_ib),
            (delete_old_prices,           only_session),
            (delete_old_positions,        only_session),
            (sync_positions,              only_session),
            (sync_timed_out_orders,       only_session),
            (sync_expired_trades,         only_session),
        ]
        log.debug("-"*50 + " RUNNING TRADE CYCLE")
        for func, args in funcs:
            try:
                func(*args)
                session.commit()
            except Exception as e:
                msg = "error executing core function {}: {}".format(func.__name__, e)
                log.error(msg)
                core_errors += 1
                session.rollback()
                raise
                if core_errors == 1 and not ibtrade.SHEET_TEST_MODE:
                    utils.send_notification("Core ibdb.py error", msg, 'zekebarge@gmail.com')
                elif core_errors >= 7:
                    raise Exception(msg + ". Program shut down.")
        session.close()

        sleep(EVAL_INTERVAL)


def run_ibdb_app():
    """Executes IbDbApp/Trade Evaluation threads during current (or next) market hours."""

    thread = IbAppThreaded(cls=IbDbApp)
    thread.start()
    sleep(5)

    try:
        run_ib_database(thread.app, Session)
    except Exception as e:
        msg = "SHEET_TEST_MODE: {}<br>ERROR: {}<br><br>".format(ibtrade.SHEET_TEST_MODE, e)
        utils.send_notification("Fatal ibdb.py Error", msg, 'zekebarge@gmail.com')
        log.error(msg)
        raise
        if not ibtrade.SHEET_TEST_MODE:
            raise
    finally:
        thread.app.disconnect()
        thread.join()


if __name__ == '__main__':
    run_ibdb_app()

