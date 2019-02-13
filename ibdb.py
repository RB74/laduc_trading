"""
The database layer to Interactive Brokers trade integration.
We need to have a few pieces of data stored in one place with persistence.

    1) Trades entries from Google Sheets.
    2) Prices from interactive brokers on open trades.
    3) Orders executed as trade targets are hit.
"""

import os
import pytz
import utils
import ibutils
import ibtrade
from ibapi.tag_value import TagValue
from time import sleep
import ib
from ib import IbApp, IbAppThreaded, iswrapper, tick_type_map, log
from ibtrade import Trade, get_data_entry_trades, MAP_10_SEC
from ibapi.contract import ComboLeg
from ibapi.order import Order
from collections import defaultdict
from datetime import datetime, timedelta
from sqlalchemy import create_engine, select, func, and_, or_, Column, String, Integer, Float, DateTime, ForeignKey
from sqlalchemy.orm import sessionmaker, scoped_session, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_method, hybrid_property
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm.exc import StaleDataError


CANCEL_PLACED_ORDERS_ON_START = False
# Set this to true once to cancel placed orders that didnt execute (e.g after hours)


SYNC_POSITIONS = False
# True deletes open trades from IB that don't match an open GSheet trade.


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
            ib_app.cancelMktData(self.request_id)
            self.request_id = None
            self.date_requested = None
            self.active = 0

    @hybrid_method
    def request_subscription(self, ib_app, contract: ibutils.Contract):
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
        return len([o for o in self.orders
                    if o.action == action
                    and o.exclude == 0]) > 0

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
        return "<IbTrade(symbol='{}', ul_entry='{}', target1='{}', stop1='{}', date_entered='{}',"\
                "pos_size='{}')>".format(
                    self.symbol,
                    self.underlying_entry_price,
                    self.target_price1,
                    self.stop_price1,
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
    def get_guessed_ib_contract(self):
        sec_type = self.security_type
        if sec_type == 'CASH':
            return ibutils.get_cash_contract(self.symbol)
        elif sec_type == 'STK':
            return ibutils.get_stock_contract(self.symbol)
        elif sec_type == 'OPT':
            return ibutils.get_option_contract_from_contract_key(self.contract_id)


class IbContract(Base):
    """ibapi Contracts"""
    __tablename__ = 'ib_contracts'

    id = Column(Integer, primary_key=True)
    contract_id = Column(String)
    parent_contract_id = Column(String, ForeignKey('ib_contracts.ib_contract_id'))
    ib_contract_id = Column(Integer)
    symbol = Column(String)
    exchange = Column(String, default='SMART')
    sec_type = Column(String)
    expiration = Column(String)
    strike = Column(Float)
    right = Column(String)   # C, P
    parent_contract = relationship('IbContract', remote_side=[ib_contract_id])
    child_contracts = relationship('IbContract')
    trade_legs = relationship('IbTradeLeg')

    @hybrid_property
    def ib_contract(self):
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
    def get_ib_contract(self, trade_id: int):
        """Safe way to retrieve a contract."""
        if self.sec_type == 'BAG':
            return self.get_bag_contract(trade_id)
        return self.ib_contract

    @hybrid_method
    def get_bag_contract(self, trade_id: int = None):
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
    price = Column(Float)
    qty = Column(Integer)
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
        o.totalQuantity = self.qty or 0
        o.action = self.action
        o.orderType = 'MKT'
        if self.trade.sec_type == 'BAG':
            o.smartComboRoutingParams = []
            o.smartComboRoutingParams.append(TagValue("NonGuaranteed", "1"))
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
        return """IbOrder(contract={}, qty={}, id={}, price={}, time={})""".format(
            self.contract_id, self.qty, self.request_id, self.price, self.time
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
    def ib_combo_leg(self):
        # TODO: Figure out ratios > 1?
        # I place a ratio of 1/2 and get an error message about max
        # ratio being 1/8 from IB...stupid.
        c = ComboLeg()
        c.action = self.action
        c.conId = self.ib_contract_id
        #c.ratio = 1
        c.ratio = self.ratio
        c.exchange = self.exchange
        return c

    @hybrid_method
    def get_request_contract(self):
        # Returns a contract from contract_id: 'NVDA-20190125-150.0-C'
        return ibutils.get_option_contract_from_contract_key(self.contract_id)


def call_with_session(func, *args):
    """Executes a function with a Session() as the first parameter."""
    session = Session()
    result = None
    try:
        result = func(session, *args)
        session.commit()
    except Exception as e:
        session.rollback()
        if TEST_MODE or ibtrade.SHEET_TEST_MODE:
            log.error("{}: {}".format(func.__name__, e))
            raise
        else:
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


def evaluate_trades(session, outside_rth=False):
    """Creates an IbOrder for any trade that needs to be closed (partial or full)"""

    open_orders = session.query(IbOrder.trade_id).filter(
        IbOrder.status.in_(OrderStatus.PENDING_STATUSES)).all()
    trade_ids = [o.trade_id for o in open_orders]
    recent_fills = session.query(IbOrder.trade_id).filter(
        and_(IbOrder.date_filled > datetime.utcnow() - timedelta(minutes=15),
             IbOrder.exclude == 0)
    ).all()
    trade_ids.extend([o.trade_id for o in recent_fills])

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

        if action and qty and mkt_open:
            if abs(qty) > abs(left):
                qty = left
            t.underlying_exit_price = price
            register_order(session, t, action, qty, status=OrderStatus.READY)


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


def get_orders_by_contract_id(session, contract_id, hours_ago=24):
    return session.query(IbOrder).filter(
        and_(IbOrder.contract_id == contract_id,
             IbOrder.time >= datetime.utcnow() - timedelta(hours=hours_ago))
    ).all()


def get_order_by_request_id(session, req_id):
    return session.query(IbOrder).filter(IbOrder.request_id == req_id).one_or_none()


def get_price_by_contract_id(session, contract_id, min_seconds=0):
    criteria = [IbPrice.contract_id == contract_id]
    if min_seconds > 0:
        eval_time = datetime.utcnow() - timedelta(seconds=min_seconds)
        criteria.append(IbPrice.time >= eval_time)
        condition = and_(*criteria)
    else:
        condition = criteria[0]
    return session.query(IbPrice).filter(condition).order_by(IbPrice.time.desc()).first()


def get_trade_by_uid(session, uid):
    return session.query(IbTrade).filter(IbTrade.u_id == str(uid)).one_or_none()


def get_trade_market_price(session, trade, ib_app, raise_gsheet_error=True):
    """
    Returns the current market price of a trade (BAG or otherwise)
    :param session:
    :param trade:
    :param ib_app:
    :param raise_gsheet_error:
    :return:
    """
    if trade.sec_type == 'BAG':
        legs = trade.legs
        price = 0
        for leg in legs:
            p = get_price_by_contract_id(session, leg.contract_id, min_seconds=60)
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
                price += p.price
            else:
                price -= p.price
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
        price = p.price
    return price


def maybe_request_executions(session, ib_app):
    max_exec = session.query(func.max(IbExecution.utc_time)).scalar()
    max_order = session.query(func.max(IbOrder.date_added)).filter(
        IbOrder.status == OrderStatus.PLACED).scalar()
    if not max_order:
        return
    if not max_exec or max_order >= max_exec:
        ib_app.request_executions()
        sleep(3)


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

    from ibapi.order import Order

    for order in orders:
        market_open = stocks_open if order.contract.sec_type == 'STK' else options_open
        if not market_open:
            continue
        contract = order.contract.get_ib_contract(order.trade_id)
        combo_legs = getattr(contract, 'comboLegs', None)
        if combo_legs:
            con_ids = [c.conId for c in combo_legs if c.conId]
            if len(con_ids) != len(combo_legs):
                continue
        log.debug("contract: {}, combo legs: {}".format(contract, combo_legs))

        ib_order = Order()
        ib_order.action = order.action
        ib_order.orderType = 'MKT'
        ib_order.totalQuantity = abs(order.qty or 0)
        if not options_open:
            ib_order.outsideRth = True

        order.request_id = ib_app.next_id()
        order.status = OrderStatus.PLACED
        session.commit()

        ib_app.placeOrder(order.request_id, contract, ib_order)

    sleep(1)
    ib_app.request_executions()


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
        log.error(e)
        session.rollback()


def register_mkt_data_activity(session, req_id):
    subscription = session.query(IbMktDataSubscription).filter(
        IbMktDataSubscription.request_id == req_id
    ).one_or_none()
    if subscription:
        subscription.active_date = datetime.utcnow()


def register_order(session, trade, action, qty, request_id=None, exclude=0, status=OrderStatus.READY):
    o = IbOrder()
    o.u_id = trade.u_id
    o.action = action
    o.qty = qty
    o.status = status
    o.trade_id = trade.id
    o.contract_id = trade.contract_id
    o.exclude = exclude
    o.request_id = request_id
    session.add(o)
    session.commit()
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
        match = IbPosition()
        match.contract_id = contract.key
        match.symbol = contract.symbol
        match.position = position
        match.account_name = account_name
        match.security_type = contract.secType
        session.add(match)
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


def sync_gsheet_trades(session):
    """Updates the database with trades from the GSheet."""
    ibtrade.log.debug("Syncing gsheet_trades.")
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


def sync_opening_orders(session):
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
        register_order(session, t, action, t.total_qty, status=OrderStatus.READY)


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
    for trade in trades:
        if trade.date_exited:
            trade.status = TradeStatus.CLOSED
            continue

        og_entry = trade.original_entry_price
        if not og_entry:
            # Nothing to pre-check, release trade.
            trade.status = TradeStatus.OPEN
            continue

        if not trade.has_valid_legs:
            # Eventually we should get legs
            continue

        # Get price of security
        price = get_trade_market_price(session, trade, ib_app, raise_gsheet_error=True)
        if not price:
            continue

        diff = abs(abs(og_entry) - abs(price))

        if abs(diff) > 0.05*abs(price):
            msg = IbTradeMessage()
            msg.error_code = 99993
            msg.text = "original entry price ({}) " \
                       "is more than 5% away from market price ({})".format(og_entry, price)
            register_trade_msg(trade, msg, update_gsheet=True)
        else:
            trade.status = TradeStatus.OPEN
    session.commit()


def sync_price_subscriptions(session, ib_app, outside_rth=False):
    """Checks missing price subscriptions on open IbTrades and attempts to subscribe."""
    open_secs = utils.get_seconds_to_market_open()

    current_prices = session.query(IbPrice).filter(
        IbPrice.time > datetime.utcnow() - timedelta(minutes=10)).all()
    current_contract_ids = list(set([p.contract_id for p in current_prices]))

    stocks_missing = session.query(IbTrade).filter(
        and_(IbTrade.underlying_contract_id.notin_(current_contract_ids),
             IbTrade.date_exited.is_(None),
             IbTrade.registration_attempts <= 3,
             IbTrade.status == TradeStatus.OPEN)
    ).all()
    missing_ids = [t.underlying_contract_id for t in stocks_missing]
    subscriptions = session.query(IbMktDataSubscription).filter(
        IbMktDataSubscription.contract_id.in_(missing_ids)).all()

    subscriptions_map = {s.contract_id: s for s in subscriptions}
    u_ids = list()

    # Add new subscriptions
    for trade in stocks_missing:
        if trade.u_id in u_ids or not trade.has_valid_legs:
            continue

        u_ids.append(trade.u_id)
        sheet_trade = trade.gsheet_trade
        c = sheet_trade.get_stock_contract()
        try:
            sub = subscriptions_map[trade.underlying_contract_id]
            if sub.date_requested and sub.date_requested > datetime.utcnow() - timedelta(minutes=15):
                # Don't request prices more than every 15 minutes.
                # TODO: Don't request prices at stupid hours.
                continue
        except KeyError:
            sub = IbMktDataSubscription()
            sub.contract_id = trade.underlying_contract_id
            sub.ib_contract_id = trade.underlying_contract_pk
            sub.date_requested = datetime.utcnow()
            session.add(sub)
            subscriptions_map[sub.contract_id] = sub

        sub.refresh_subscription(ib_app, c)
        current_contract_ids.append(c.key)

    # Drop inactive subscriptions
    active_trades = session.query(IbTrade).filter(
        and_(IbTrade.date_exited.is_(None),
             IbTrade.registration_attempts <= 3,
             IbTrade.status == TradeStatus.OPEN
             )
    ).all()

    active_trade_map = {t.underlying_contract_id : t for t in active_trades}
    active_keys = list(active_trade_map.keys())
    drop_keys = [k for k in current_contract_ids if k not in active_keys]
    qry = session.query(IbMktDataSubscription).filter(
        and_(IbMktDataSubscription.contract_id.in_(drop_keys),
             IbMktDataSubscription.active == 1)
    )

    for sub in qry.all():
        sub.cancel_subscription(ib_app)

    session.commit()


def sync_timed_out_orders(session):
    mins_to_mkt = utils.get_minutes_to_market_open()
    if mins_to_mkt > 0:
        return  # Don't process outside of market hours.
    elif 0 > mins_to_mkt > -30:
        return   # Allow ~30 mins of market time to fill open orders.

    orders = session.query(IbOrder).filter(
        and_(IbOrder.status.in_(OrderStatus.PENDING_STATUSES),
             IbOrder.date_added < datetime.utcnow() - timedelta(minutes=15))
    ).all()

    for order in orders:
        order.status = OrderStatus.ERROR
        trade = order.trade

        if trade is None:
            continue

        trade.status = TradeStatus.ERROR
        msg = IbTradeMessage()
        msg.error_code = 99991
        msg.text = "Execution order time out. Delete/recreate the trade."
        msg.status = MsgStatus.OPEN
        register_trade_msg(trade, msg, update_gsheet=True)

        session.commit()


def sync_fills(session):
    """Processes IbOrders from PLACED to COMPLETE, updating GSheet/IbTrade with details from IbExecutions."""
    orders = session.query(IbOrder).filter(IbOrder.status == OrderStatus.PLACED).all()
    for order in orders:
        log.debug("sync_fills: {}".format(order))

        all_execs = get_executions_by_order_id(session, order.request_id)
        execs = order.get_valid_executions(all_execs)
        qty = order.get_executed_qty(execs)
        log.debug("# of executions: {}".format(len(execs)))
        log.debug("exec qty: {}".format(qty))
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
                    if db_trade.size < 0:
                        # credit entry / debit exit
                        if abs(price) > abs(db_trade.entry_price):
                            attrs['notes'] = ibtrade.STOP_LOSS
                        else:
                            attrs['notes'] = ibtrade.TGT_REACHED

            # Assumes all BAG trades are "LONG" to IB. Even when the sub-contracts make the position
            # A technical short trade. (e.g credit trades)
            if db_trade.sec_type == 'BAG':
                key = 'entry_price' if order.action == 'BUY' else 'exit_price'
                if db_trade.size < 0:
                    attrs[key] = -abs(attrs[key])
                else:
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


def sync_invalid_trades(session):

    msgs = session.query(IbTradeMessage).filter(
        and_(IbTradeMessage.status == MsgStatus.OPEN,
             IbTradeMessage.error_code > 0)
    ).join(IbTrade).filter(
        IbTrade.status.notin_([TradeStatus.CLOSED, TradeStatus.ERROR])
    ).all()

    if not msgs:
        return

    log.debug("sync_invalid_trades: {} messages".format(len(msgs)))
    highlight_codes = ib.CODES_USER_ERROR + ib.CODES_PROGRAMMING_ERROR

    for msg in msgs:
        trade = msg.trade
        code = msg.error_code
        msg.status = MsgStatus.UNKNOWN

        if code in ib.CODES_IGNORE:
            continue
        elif code in highlight_codes or 3 < msg.count < 5:
            trade.status = TradeStatus.ERROR
            row = ibtrade.get_sheet_row_by_uid(trade.u_id)
            ibtrade.highlight_cell(trade.u_id, 4, row, 'red')

            if code in ib.CODES_PROGRAMMING_ERROR:
                utils.send_notification("IB Error", msg.__repr__(), 'zekebarge@gmail.com')

        elif code in ib.CODES_IB_INTERNAL:
            utils.send_notification("IB Error", msg.__repr__(), 'zekebarge@gmail.com')
        else:
            continue

        msg.status = MsgStatus.RESOLVED
        msg.date_resolved = datetime.utcnow()

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
    if MAP_10_SEC.get(throttle_key, None):
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

    MAP_10_SEC[throttle_key] = True

    mid = (bid+ask)/2
    if contract.secType in ('STK', 'OPT', 'BAG'):
        mid = round(mid, 2)

    data['mid'] = mid
    data['mid_time'] = min(bid_time, ask_time)

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

    req_contract = o.get_request_contract()
    assoc_contract = register_contract(session, req_contract)
    session.expire(assoc_contract)
    o.contract_pk = assoc_contract.id
    session.commit()


def _register_trade_from_trade_obj(session, trade: Trade) -> IbTrade:
    if not trade.tactic_parsed:
        trade.parse_tactic()

    if trade.u_id:
        match = get_trade_by_uid(session, trade.u_id)
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

    t.status = TradeStatus.PRE_OPEN_CHECK

    session.add(t)
    session.commit()

    contract1 = trade.get_contract()
    register_contract(session, contract1, trade=t)
    if contract1.secType != 'STK':
        contract2 = trade.get_stock_contract()
        register_contract(session, contract2, trade=t)

    return t


def _reset_trade(session, sql_trade: IbTrade, sheet_trade: Trade):
    """Force resets an IbTrade. The order is cancelled and it is treated as a new trade."""
    _force_close_trade(session, sql_trade)
    confirmed_row = ibtrade.get_sheet_row_by_uid(sheet_trade.u_id)
    sheet_trade.row_idx = confirmed_row
    sheet_trade.u_id = None
    session.commit()
    return _register_trade_from_trade_obj(session, sheet_trade)


def _force_close_trade(session, sql_trade, exclude=1):
    close_action = 'SELL' if sql_trade.is_long else 'BUY'
    open_action = 'BUY' if close_action == 'SELL' else 'SELL'
    statuses = (OrderStatus.COMPLETE, OrderStatus.PLACED)
    orders = [o for o in sql_trade.orders
              if o.exclude == 0 and o.status in statuses]
    open_orders = [o for o in orders if o.action == open_action]
    close_orders = [o for o in orders if o.action == close_action]

    qty_opened = 0 if not open_orders else sum([o.qty for o in open_orders])
    qty_closed = 0 if not close_orders else sum([o.qty for o in close_orders])
    qty_left = abs(qty_opened) - abs(qty_closed)

    if qty_left:
        register_order(session, sql_trade, close_action, qty_left, exclude=exclude)


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
    t.u_id = trade.u_id
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
        now = pytz.timezone('UTC').localize(datetime.utcnow())

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
            (sync_opening_orders,         only_session),
            (evaluate_trades,             only_session),
            (place_orders,              session_and_ib),
            (maybe_request_executions,  session_and_ib),
            (sync_fills,                  only_session),
            (sync_invalid_trades,         only_session),
            (delete_old_prices,           only_session),
            (delete_old_positions,        only_session),
            (sync_positions,              only_session),
            (sync_timed_out_orders,       only_session)
        ]

        for func, args in funcs:
            try:
                func(*args)
                session.commit()
            except Exception as e:
                msg = "error executing core function {}: {}".format(func.__name__, e)
                log.error(msg)
                core_errors += 1
                session.rollback()

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
        if not ibtrade.SHEET_TEST_MODE:
            raise
    finally:
        thread.app.disconnect()
        thread.join()


if __name__ == '__main__':
    run_ibdb_app()







