"""
The database layer to Interactive Brokers trade integration.
We need to have a few pieces of data stored in one place with persistence.

    1) Trades entries from Google Sheets.
    2) Prices from interactive brokers on open trades.
    3) Orders executed as trade targets are hit.
"""

import os
import utils
import ibutils
import ibtrade
from time import sleep
from ibtrade import Trade, get_data_entry_trades, MAP_10_SEC
from ibapi.contract import ComboLeg
from ibapi.order import Order
from datetime import datetime, timedelta
from sqlalchemy import create_engine, and_, Column, String, Integer, Float, DateTime, ForeignKey
from sqlalchemy.orm import sessionmaker, scoped_session, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_method, hybrid_property


class OrderStatus:
    READY = 'ready'
    PLACED = 'placed'
    COMPLETE = 'complete'
    ERROR = 'error'
    PENDING_STATUSES = [READY, PLACED]


DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
DB_PATH = os.path.join(DATA_DIR, 'ib.db')
EVAL_INTERVAL = utils.config['ib'].getint('eval_interval', 30)
TEST_MODE = utils.config['ib'].getboolean('test_mode', True)

Base = declarative_base()

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

engine = create_engine("sqlite:///" + DB_PATH)
Session = scoped_session(sessionmaker(bind=engine))


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
    date_entered = Column(DateTime)
    date_exited = Column(DateTime)
    u_id = Column(String)

    parent_trade_id = Column(Integer, ForeignKey('ib_trades.id'))
    contract_id = Column(String, ForeignKey('ib_contracts.contract_id'))
    underlying_contract_id = Column(String, ForeignKey('ib_contracts.contract_id'))
    date_added = Column(DateTime, default=datetime.utcnow)
    registration_attempts = Column(Integer, default=0)

    legs = relationship("IbTradeLeg")
    orders = relationship("IbOrder", back_populates='trade')
    contract = relationship('IbContract', foreign_keys=[contract_id])
    underlying_contract = relationship('IbContract', foreign_keys=[underlying_contract_id])
    parent_trade = relationship('IbTrade')

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
    def is_short(self):
        return self.size < 0

    @hybrid_property
    def is_long(self):
        return self.size > 0

    @hybrid_property
    def total_qty(self):
        size = self.size
        entry = self.entry_price
        if not entry and self.original_entry_price:
            entry = abs(self.original_entry_price)
        if not all((entry, size)):
            return None
        size *= 1000
        if self.sec_type in ('BAG', 'OPT'):
            entry *= 100
        return round(abs(size/entry), 0)

    @hybrid_property
    def stop_qty(self):
        return round(self.total_qty/len(self.stop_prices), 0)

    @hybrid_property
    def target_qty(self):
        return round(self.total_qty/len(self.target_prices), 0)

    @hybrid_property
    def bought_qty(self):
        return sum([order.qty for order in self.orders
                    if order.action == 'BUY'
                    and order.status == OrderStatus.COMPLETE
                    and order.exclude == 0])

    @hybrid_property
    def sold_qty(self):
        return sum([abs(order.qty) for order in self.orders
                    if order.action == 'SELL'
                    and order.status == OrderStatus.COMPLETE
                    and order.exclude == 0])

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
    def get_next_target(self):
        action = 'BOT' if self.is_short else 'SLD'
        orders = [order for order in self.orders
                  if order.action == action
                  and order.status != OrderStatus.ERROR
                  and order.exclude == 0]
        idx = len(orders)

        try:
            return idx, self.target_prices[idx]
        except IndexError:
            return None, None

    @hybrid_method
    def get_next_stop(self):
        action = 'BOT' if self.is_short else 'SLD'
        orders = [order for order in self.orders
                  if order.action == action
                  and order.status != OrderStatus.ERROR
                  and order.exclude == 0]
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


class IbPosition(Base):
    """ibapi portfolio records."""

    __tablename__ = 'ib_positions'
    contract_id = Column(String, primary_key=True)
    symbol = Column(String)
    security_type = Column(String)
    position = Column(Float)
    market_price = Column(Float)
    account_name = Column(String, primary_key=True)
    time = Column(DateTime)



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
    orders = relationship('IbOrder', back_populates='contract')

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
        legs = self.trade_legs
        if trade_id:
            legs = [leg for leg in legs if leg.trade_id == trade_id]
        else:
            trade_ids = list(set([leg.trade_id for leg in legs]))
            if len(trade_ids) > 1:
                raise AttributeError("IbContract {} has multiple trade_ids associated. "
                                     "Provide the trade_id param to IbContract.get_bag_contract() "
                                     "to avoid this error.".format(self))

        for leg in sorted(legs, key=lambda t: t.sequence):
            ib_leg = leg.ib_combo_leg
            symbols.append(ib_leg.symbol)
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
    order_id = Column(Integer, ForeignKey('ib_orders.request_id'))
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


class IbOrder(Base):
    """ibapi Orders"""
    __tablename__ = 'ib_orders'

    id = Column(Integer, primary_key=True)
    trade_id = Column(Integer, ForeignKey('ib_trades.id'))
    contract_id = Column(String, ForeignKey('ib_contracts.contract_id'))
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
    executions = relationship("IbExecution")
    trade = relationship("IbTrade", back_populates='orders')
    contract = relationship("IbContract")
    exclude = Column(Integer, default=0)

    @hybrid_property
    def ib_order(self) -> Order:
        o = Order()
        o.totalQuantity = self.qty
        o.action = self.action
        o.orderType = 'MKT'
        return o

    @hybrid_method
    def get_valid_executions(self) -> list:
        """Returns unique executions with the latest correction"""
        execs = dict()

        for e in self.executions:
            try:
                store = execs[e.base_exec_id]
                store.append(e)
            except KeyError:
                store = execs[e.base_exec_id] = list()
                store.append(e)

        valid = list()
        for base_id, store in execs.items():
            if len(store) == 1:
                valid.append(store[0])
            else:
                latest = list(sorted(store, key=lambda x: x.correction_id))[-1]
                valid.append(latest)

        return valid


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
    contract_id = Column(String, ForeignKey('ib_contracts.contract_id'))
    ib_contract_id = Column(Integer)
    expiration = Column(Integer)
    date_added = Column(DateTime, default=datetime.utcnow)
    date_requested = Column(DateTime)

    trade = relationship("IbTrade", back_populates='legs')
    contract = relationship('IbContract')

    @hybrid_property
    def ib_combo_leg(self):
        c = ComboLeg()
        c.action = self.action
        c.conId = self.ib_contract_id
        c.ratio = self.ratio
        c.expiration = str(self.expiration)
        c.exchange = self.exchange
        return c


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
            print(e)
        else:
            raise
    finally:
        session.close()

    return result


def delete_old_prices(session):
    condition = IbPrice.time < datetime.utcnow() - timedelta(minutes=20)
    session.query(IbPrice).filter(condition).delete(synchronize_session=False)


def delete_trade_legs(session, trade_id):
    session.query(IbTradeLeg).filter(IbTradeLeg.trade_id == trade_id).delete(synchronize_session=False)


def evaluate_trades(session, outside_rth=False):
    """Creates an IbOrder for any trade that needs to be opened, partially-closed, or closed."""
    if not outside_rth and not utils.now_is_rth():
        return

    open_orders = session.query(IbOrder).filter(IbOrder.status == OrderStatus.PLACED).all()
    trade_ids = [o.trade_id for o in open_orders]
    trades = session.query(IbTrade).filter(
        and_(IbTrade.date_exited == None,
             IbTrade.u_id != None,
             IbTrade.underlying_contract_id != None,
             IbTrade.id.notin_(trade_ids))).all()

    for t in trades:
        if not t.has_opening_order:
            if t.total_qty:
                action = 'SELL' if t.is_short else 'BUY'
                register_order(session, t, action, t.total_qty, status=OrderStatus.READY)
            continue

        p = get_price_by_contract_id(session, t.underlying_contract_id, min_seconds=60)
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

        if action and qty:
            if abs(qty) > abs(left):
                qty = left
            register_order(session, t, action, qty, status=OrderStatus.READY)


def get_trade_diffs(sql_trade, sheet_trade):
    diffs = list()
    columns = ['symbol', 'size', 'expiry_month', 'expiry_day',
               'expiry_year', 'strike', 'tactic', 'alert_category', 'entry_price']
    for c in columns:
        sql_val = getattr(sql_trade, c, None)
        sheet_val = getattr(sheet_trade, c, None)
        if sheet_val and sql_val != sheet_val:
            diffs.append((c, sql_val, sheet_val))
    return diffs


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


def maybe_update_trade(sql_trade: IbTrade, sheet_trade):
    diffs = get_trade_diffs(sql_trade, sheet_trade)

    if not diffs:
        return False

    new_diffs = list()
    for field, sql_val, sheet_val in diffs:
        if field == 'entry_price':
            if sql_trade.size < 1 and abs(sql_val) == abs(sheet_val):
                continue
        new_diffs.append((field, sql_val, sheet_val))

    if not new_diffs:
        return False

    for field, orig_value, new_value in new_diffs:
        if field == 'entry_price':
            if new_value > 0:
                sql_trade.size = abs(sql_trade.size)
            else:
                sql_trade.size = -abs(sql_trade.size)

        setattr(sql_trade, field, new_value)

    return new_diffs


def place_orders(session, ib_app):
    orders = session.query(IbOrder).filter(
        and_(IbOrder.status == OrderStatus.READY,
             IbOrder.request_id == None)
    ).all()

    if not orders:
        return

    from ibapi.order import Order

    for order in orders:
        contract = order.contract.get_trade_contract(order.trade.id)
        combo_legs = getattr(contract, 'comboLegs', None)
        if combo_legs:
            con_ids = [c.conId for c in combo_legs if c.conId]
            if len(con_ids) != len(combo_legs):
                continue

        ib_order = Order()
        ib_order.action = order.action
        ib_order.orderType = 'MKT'
        ib_order.totalQuantity = abs(order.qty)

        order.request_id = ib_app.next_id()
        order.status = OrderStatus.PLACED
        session.commit()

        ib_app.placeOrder(order.request_id, contract, ib_order)

    if orders:
        sleep(3)
        ib_app.request_executions()


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

    if trade is not None and trade.sec_type == 'BAG':
        sheet_trade = trade.gsheet_trade
        sheet_trade.get_contract()
        for i, leg in enumerate(sheet_trade.leg_data, start=1):
            _register_trade_leg(trade, leg, i)

    session.commit()

    return c


def register_contract_details(session, contract, details):
    contracts = session.query(IbContract).filter(IbContract.contract_id == contract.key).all()
    detail = details[-1]
    for contract in contracts:
        contract.ib_contract_id = detail.underConId
    return contracts


def register_executions(session, executions):
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

            session.add(match)
            session.commit()

        matches[match.exec_id] = match
    return matches


def register_ib_contract_id(session, contract_id, ib_contract_id):
    condition = and_(IbTradeLeg.contract_id == contract_id,
                     IbTradeLeg.ib_contract_id == None)
    legs = session.query(IbTradeLeg).filter(condition).all()
    for leg in legs:
        leg.ib_contract_id = ib_contract_id
    session.commit()


def register_ib_price(session, contract, price_data):
    p = IbPrice()
    p.contract_id = contract.key
    p.bid = price_data['bid']
    p.ask = price_data['ask']
    p.price = price_data['mid']
    p.time = price_data['bid_time']
    session.add(p)


def register_order(session, trade, action, qty, exclude=0, status=OrderStatus.READY):
    o = IbOrder()
    o.u_id = trade.u_id
    o.action = action
    o.qty = qty
    o.status = status
    o.trade_id = trade.id
    o.contract_id = trade.contract_id
    o.exclude = exclude
    session.add(o)
    session.commit()


def register_positions(session, account_name, portfolio):
    matches = {p.contract_id: p for p in session.query(IbPosition).filter(
               IbPosition.account_name == account_name).all()}

    for contract_id, data in portfolio.items():
        try:
            match = matches[contract_id]
            match.position = data['position']
            match.market_price = data['market_price']
            match.time = datetime.utcnow()
        except KeyError:
            match = IbPosition()
            match.symbol = data['symbol']
            match.security_type = data['security_type']
            match.position = data['position']
            match.market_price = data['market_price']
            match.account_name = data['account_name']
            match.contract_id = contract_id
            session.add(match)

    if portfolio:
        session.commit()


def register_trade(session, trade):
    if hasattr(trade, '__iter__'):
        trade = Trade.from_gsheet_row(trade)
    return _register_trade_from_trade_obj(session, trade)


def request_ib_contract_ids(session, ib_app):
    """Requests IbTradeLeg.ib_contract_id from ibapi. This makes BAG trades possible."""
    legs = session.query(IbTradeLeg).filter(
        and_(IbTradeLeg.ib_contract_id == None,
             IbTradeLeg.registration_attempts <= 3)).all()

    for leg in legs:
        contract = leg.contract
        ib_app.request_contract_id(contract.ib_contract)
        leg.date_requested = datetime.utcnow()
        leg.registration_attempts += 1

    session.commit()


def sync_gsheet_trades(session, trade_callbacks=None):
    """Updates the database with trades from the GSheet."""
    trades = get_data_entry_trades()

    if trade_callbacks:
        for callback in trade_callbacks:
            if not callable(callback):
                callback, args = callback
                callback(*args, trades)
            else:
                callback(trades)

    for trade in trades:
        register_trade(session, trade)


def sync_positions(session):
    positions = session.query(IbPosition).filter(
        and_(IbPosition.time > datetime.utcnow() - timedelta(minutes=1),
             IbPosition.position != 0)).all()

    for pos in positions:
        trades = session.query(IbTrade).filter(
            and_(IbTrade.contract_id == pos.contract_id,
                 IbTrade.date_exited == None,
                 IbTrade.entry_price > 0)).all()

        for t in trades:
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
                msg = "{} is off by {}".format(t, qty)
                if TEST_MODE or ibtrade.SHEET_TEST_MODE:
                    print(msg)
                    register_order(session, t, action, qty, exclude=1)
                else:
                    raise Exception(msg)


def sync_price_subscriptions(session, ib_app, outside_rth=False):
    """Checks missing price subscriptions on open IbTrades and attempts to subscribe."""
    if not outside_rth and not utils.now_is_rth():
        return

    current_prices = session.query(IbPrice).filter(
        IbPrice.time > datetime.utcnow() - timedelta(minutes=10)).all()
    current_contract_ids = list(set([p.contract_id for p in current_prices]))

    stocks_missing = session.query(IbTrade).filter(
        and_(IbTrade.underlying_contract_id.notin_(current_contract_ids),
             IbTrade.date_exited == None,
             IbTrade.registration_attempts <= 3)
    ).all()
    others_missing = session.query(IbTrade).filter(
        and_(IbTrade.contract_id.notin_(current_contract_ids),
             IbTrade.date_exited == None,
             #IbTrade.sec_type != 'BAG'
             IbTrade.registration_attempts <= 3
             )
    ).all()
    all_missing = list(stocks_missing) + list(others_missing)
    u_ids = list()

    for trade in all_missing:
        if trade.u_id in u_ids:
            continue
        if trade.sec_type == 'BAG':
            legs = trade.legs
            valid_legs = [leg.ib_contract_id for leg in trade.legs if leg.ib_contract_id]
            if len(valid_legs) != len(legs):
                continue
        u_ids.append(trade.u_id)
        ib_app.register_trade(trade.gsheet_trade, force=True)
        trade.registration_attempts += 1

    session.commit()


def sync_fills(session):
    orders = session.query(IbOrder).filter(IbOrder.status == OrderStatus.PLACED).all()
    for order in orders:

        execs = order.get_valid_executions()
        qty = sum([e.shares for e in execs])

        if abs(qty) < abs(order.qty):
            continue

        price = sum([e.avg_price for e in execs])/len(execs)
        db_trade = order.trade
        now_utc = max(execs, key=lambda e: e.utc_time).utc_time
        now_est = utils.utc_to_est(now_utc).strftime(ibtrade.SHEET_TIME_FMT)

        attrs = {'date_entered': None, 'date_exited': None,
                 'entry_price': None,  'exit_price': None,
                 'is_partial': None,   'pct_sold': None, 'notes': None}

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

        if attrs['is_partial']:
            sheet_trade_partial = ibtrade.close_sheet_trade_partial(
                db_trade.u_id, attrs['pct_sold'], attrs['exit_price'],
                attrs['date_exited'], attrs['notes'])
            db_trade_partial = register_trade(session, sheet_trade_partial)
            db_trade_partial.parent_trade_id = db_trade.id
        else:
            if attrs['date_exited']:
                u_id = ibtrade.close_sheet_trade(
                    db_trade.u_id, attrs['pct_sold'], attrs['exit_price'],
                    attrs['date_exited'], attrs['notes'])
                db_trade.u_id = str(u_id)
                db_trade.exit_price = abs(attrs['exit_price'])
                db_trade.date_exited = now_utc
            else:
                ibtrade.open_sheet_trade(
                    db_trade.u_id,
                    attrs['entry_price'],
                    attrs['date_entered'])
                db_trade.entry_price = abs(attrs['entry_price'])
                db_trade.date_entered = now_utc

        order.status = OrderStatus.COMPLETE
        order.date_filled = now_utc
        session.commit()


def _get_price_data_import(contract: ibutils.Contract, data: dict):
    """Calculates mid, mid_time prices. If data is returned it should be uploaded to ib_prices."""
    throttle_key = 'handle_price' + contract.key
    if MAP_10_SEC.get(throttle_key, None):
        return
    MAP_10_SEC[throttle_key] = True

    bid = data.get('bid', None)
    bid_time = data.get('bid_time', None)
    ask = data.get('ask', None)
    ask_time = data.get('ask_time', None)

    if not all((bid, ask)):
        return

    check_time = datetime.utcnow() - timedelta(seconds=30)
    if bid_time < check_time or ask_time < check_time:
        return

    mid = (bid+ask)/2
    if contract.secType in ('STK', 'OPT', 'BAG'):
        mid = round(mid, 2)

    data['mid'] = mid
    data['mid_time'] = check_time

    return data


def _register_trade_leg(trade: IbTrade, leg: dict, sequence: int):
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


def _register_trade_from_trade_obj(session, trade: Trade) -> IbTrade:
    if not trade.tactic_parsed:
        trade.parse_tactic()

    if trade.u_id:
        match = get_trade_by_uid(session, trade.u_id)
        if match:
            maybe_update_trade(match, trade)
            session.commit()
            return match

    t = IbTrade()

    if not trade.u_id:
        t.original_entry_price = trade.entry_price
        trade.init_new_trade()

    _set_sql_trade_from_gsheet_trade(t, trade)

    if trade.is_short:
        t.size = -abs(t.size)
        t.original_entry_price = -abs(t.original_entry_price)

    session.add(t)
    session.commit()

    contract1 = trade.get_contract()
    register_contract(session, contract1, trade=t)

    if contract1.secType != 'STK':
        contract2 = trade.get_stock_contract()
        register_contract(session, contract2, trade=t)

    return t


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


from ib import IbApp, IbAppThreaded, iswrapper, tick_type_map


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

        contract = self._contracts_by_req[req_id]
        data = self.prices[req_id]
        data[tick_type] = float(price)
        data[tick_type + '_time'] = datetime.utcnow()
        print("{}: {}".format(contract.key, price))
        price = _get_price_data_import(contract, data)
        if price:
            call_with_session(register_ib_price, contract, price)

    @iswrapper
    def contractDetails(self, req_id, details):
        self._contract_details[req_id].append(details)

    @iswrapper
    def contractDetailsEnd(self, req_id):
        contract = self._contracts_by_req.pop(req_id)
        details = self._contract_details.pop(req_id)
        call_with_session(register_ib_contract_id, contract.key, details[0].underConId)

    @iswrapper
    def execDetails(self, req_id: int, contract, execution):
        self.executions[req_id].append((contract, execution))

    @iswrapper
    def execDetailsEnd(self, req_id: int):
        try:
            executions = self.executions.pop(req_id)
            call_with_session(register_executions, executions)
        except KeyError:
            pass

    @iswrapper
    def accountDownloadEnd(self, account_name):
        p = self.portfolio[account_name]
        call_with_session(register_positions, account_name, p)
        p.clear()


def run_ib_database(ib_app, Session):
    """Executes trade management ibdb functions on an interval."""
    session = Session()
    Base.metadata.create_all(bind=session.bind)

    while utils.get_seconds_to_market_open() < 0:
        session = Session()

        sync_gsheet_trades(session)
        request_ib_contract_ids(session, ib_app)
        sync_price_subscriptions(session, ib_app)
        evaluate_trades(session)
        place_orders(session, ib_app)
        sync_fills(session)
        sync_positions(session)
        delete_old_prices(session)

        session.commit()
        session.close()

        sleep(EVAL_INTERVAL)


def run_ibdb_app():
    """Executes IbDbApp/Trade Evaluation threads during current (or next) market hours."""
    seconds = utils.get_seconds_to_market_open()
    if seconds > 0:
        print("run_ibdb_app: {} minutes ({} hours) "
              "until market open. Starting then.".format(
               seconds/60, seconds/60/60))
        sleep(seconds)

    thread = IbAppThreaded(cls=IbDbApp)
    thread.start()
    sleep(5)
    run_ib_database(thread.app, Session)


if __name__ == '__main__':
    run_ibdb_app()







