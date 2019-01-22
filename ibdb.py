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
import functools
from time import sleep
from ibtrade import Trade, get_data_entry_trades, TradeSheet, MAP_10_SEC
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


EVAL_INTERVAL = utils.config['ib'].getint('eval_interval', 30)
Base = declarative_base()


class IbContract(Base):
    __tablename__ = 'ib_contracts'

    id = Column(Integer, primary_key=True)
    contract_id = Column(String)
    parent_contract_id = Column(String, ForeignKey('ib_contracts.ib_contract_id'))
    ib_contract_id = Column(Integer)
    symbol = Column(String)
    sec_type = Column(String)
    parent_contract = relationship('IbContract', remote_side=[ib_contract_id])
    child_contracts = relationship('IbContract', backref='children')


class IbPrice(Base):
    __tablename__ = 'ib_prices'

    contract_id = Column(String, nullable=False, primary_key=True)
    time = Column(DateTime, nullable=False, primary_key=True)
    price = Column(Float, nullable=False, primary_key=True)
    bid = Column(Float)
    ask = Column(Float)

class IbExecution(Base):
    __tablename__ = 'ib_executions'

    exec_id = Column(String, primary_key=True)
    order_id = Column(Integer, ForeignKey('ib_orders.request_id'))
    client_id = Column(Integer)
    server_time = Column(String)
    acc_number = Column(String)
    exchange = Column(String)
    side = Column(String)
    shares = Column(Float)
    price = Column(Float)
    cum_qty = Column(Float)
    avg_price = Column(Float)


class IbOrder(Base):
    __tablename__ = 'ib_orders'

    id = Column(Integer, primary_key=True)
    trade_id = Column(Integer, ForeignKey('ib_trades.id'))
    request_id = Column(Integer)
    u_id = Column(String)
    time = Column(DateTime)
    contract_id = Column(String)
    symbol = Column(String)
    action = Column(String)
    price = Column(Float)
    qty = Column(Integer)
    status = Column(String)
    date_added = Column(DateTime, default=datetime.utcnow)
    date_filled = Column(DateTime)
    executions = relationship("IbExecution")

    @property
    def ib_order(self):
        o = Order()
        o.totalQuantity = self.qty
        o.action = self.action
        o.orderType = 'MKT'
        return o


class IbTradeLeg(Base):
    __tablename__ = 'ib_trade_legs'
    id = Column(Integer, primary_key=True)
    trade_id = Column(Integer, ForeignKey('ib_trades.id'))
    exchange = Column(String)
    u_id = Column(String)
    symbol = Column(String)
    action = Column(String)
    ratio = Column(Integer)
    sequence = Column(Integer)
    contract_id = Column(String, ForeignKey('ib_contracts.contract_id'))
    ib_contract_id = Column(Integer)
    expiration = Column(Integer)
    date_added = Column(DateTime, default=datetime.utcnow)
    date_requested = Column(DateTime)
    trade = relationship("IbTrade")
    contract = relationship('IbContract')

    @property
    def ib_combo_leg(self):
        c = ComboLeg()
        c.action = self.action
        c.conId = self.ib_contract_id
        c.ratio = self.ratio
        c.expiration = str(self.expiration)
        c.exchange = self.exchange
        return c


class IbTrade(Base):
    __tablename__ = 'ib_trades'

    id = Column(Integer, primary_key=True)
    alert_category = Column(String)
    symbol = Column(String)
    exchange = Column(String)
    size = Column(Float)
    sec_type = Column(String)
    expiry_month = Column(Integer)
    expiry_day = Column(Integer)
    expiry_year = Column(Integer)
    strike = Column(Float)
    tactic = Column(String)
    underlying_entry_price = Column(Float)
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
    contract_id = Column(String, ForeignKey('IbContract.contract_id'))
    underlying_contract_id = Column(String, ForeignKey('IbContract.contract_id'))
    date_added = Column(DateTime, default=datetime.utcnow)

    legs = relationship("IbTradeLeg")
    orders = relationship("IbOrder")
    contract = relationship('IbContract')
    underlying_contract = relationship('IbContract')

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
        return len([o for o in self.orders if o.status == OrderStatus.READY]) > 0

    @hybrid_property
    def has_opening_order(self):
        action = 'BOT' if self.is_long else 'SLD'
        return len([o for o in self.orders if o.action == action]) > 0

    @hybrid_property
    def is_short(self):
        return self.size < 0

    @hybrid_property
    def is_long(self):
        return self.size > 0

    @hybrid_property
    def total_qty(self):
        entry = self.entry_price
        size = self.size * 1000
        if self.sec_type in ('BAG', 'OPT'):
            entry *= 100
        return round(size/entry, 2)

    @hybrid_property
    def stop_qty(self):
        return round(self.total_qty/len(self.stop_prices), 0)

    @hybrid_property
    def target_qty(self):
        return round(self.total_qty/len(self.target_prices), 0)

    @hybrid_property
    def bought_qty(self):
        return sum([order.qty for order in self.orders
                    if order.action == 'BOT'
                    and order.status == OrderStatus.COMPLETE])

    @hybrid_property
    def sold_qty(self):
        return sum([abs(order.qty) for order in self.orders
                    if order.action == 'SLD'
                    and order.status == OrderStatus.COMPLETE])

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
                  and order.status != OrderStatus.ERROR]
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
                  and order.status != OrderStatus.ERROR]
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


DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

DB_PATH = os.path.join(DATA_DIR, 'ib.db')

engine = create_engine("sqlite:///" + DB_PATH)
Session = scoped_session(sessionmaker(bind=engine))


def call_with_session(func, *args):
    session = Session()
    try:
        result = func(session, *args)
    except:
        session.rollback()
        raise
    else:
        session.commit()

    session.close()
    return result


def delete_old_prices(session):
    session.query(IbPrice).filter(
        IbPrice.time < datetime.utcnow() - timedelta(minutes=5)
    ).delete(synchronize_session=False)


def delete_trade_legs(session, trade_id):
    session.query(IbTradeLeg).filter(IbTradeLeg.trade_id == trade_id).delete(synchronize_session=False)


def evaluate_trades(session):
    trades = session.query(IbTrade).filter(
        and_(IbTrade.date_exited == None,
             IbTrade.u_id != None,
             IbTrade.underlying_contract_id != None)).all()

    for t in trades:
        if t.has_pending_order:
            continue
        if not t.has_opening_order:
            action = 'SELL' if t.is_short else 'BUY'
            register_order(session, t, action, t.total_qty, status=OrderStatus.READY)
            continue

        price = get_price_by_contract_id(session, t.underlying_contract_id, min_seconds=30)
        if price is None:
            continue

        target_idx, target_price = t.get_next_target()
        stop_idx, stop_price = t.get_next_stop()

        if target_price is None:
            continue

        action, qty = None, None
        bought = t.bought_qty
        sold = t.sold_qty

        if t.is_long:
            left = abs(bought) - abs(sold)
            if left <= 0:
                continue
            if price >= target_price:
                action = 'SELL'
                qty = t.target_qty
            elif stop_price and price <= stop_price:
                action = 'SELL'
                qty = t.stop_qty
        else:
            left = -abs(sold) + abs(bought)
            if left >= 0:
                continue
            if price <= target_price:
                action = 'BUY'
                qty = t.target_qty
            elif stop_price and price >= stop_price:
                action = 'BUY'
                qty = t.stop_qty

        if action and qty:
            if abs(qty) > abs(left):
                qty = left
            register_order(session, t, action, qty, status=OrderStatus.READY)


def get_trade_diffs(sql_trade, sheet_trade):
    diffs = list()
    columns = ['symbol', 'size', 'expiry_month', 'expiry_day',
               'expiry_year', 'strike', 'tactic', 'alert_category']
    for c in columns:
        sql_val = getattr(sql_trade, c, None)
        sheet_val = getattr(sheet_trade, c, None)
        if sql_val != sheet_val:
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


def maybe_update_trade(sql_trade, sheet_trade):
    diffs = get_trade_diffs(sql_trade, sheet_trade)

    if not diffs:
        return False

    for field, orig_value, new_value in diffs:
        setattr(sql_trade, field, new_value)

    return diffs


def place_orders(session, ib_app):
    orders = session.query(IbOrder).filter(
        IbOrder.status == OrderStatus.READY
    ).all()

    if not orders:
        return

    from ibapi.order import Order

    for order in orders:
        sheet_trade = order.trade.gsheet_trade
        contract = sheet_trade.get_contract()

        ib_order = Order()
        ib_order.action = order.action
        ib_order.orderType = 'MKT'
        ib_order.totalQuantity = order.qty

        order.request_id = ib_app.get_next_id()
        order.status = OrderStatus.PLACED
        session.commit()

        ib_app.placeOrder(order.request_id, contract, ib_order)


def register_contract(session, contract, parent_id=None):
    """Registers a new contract (and sub-contracts) """
    if parent_id is not None:
        args = and_(IbContract.contract_id == contract.key,
                    IbContract.parent_contract_id == parent_id)
    else:
        args = IbContract.contract_id = contract.key
    c = session.query(IbContract).filter(args).one_or_none()

    if c is None:
        c = IbContract()
        c.contract_id = contract.key
        c.sec_type = contract.secType
        c.symbol = contract.symbol
        c.parent_contract_id = parent_id
        session.add(c)
        session.commit()

    combo_legs = getattr(c, 'comboLegs', [])

    for leg in combo_legs:
        register_contract(session, leg['contract'], c.contract_id)

    return c


def register_contract_details(session, contract, details):
    contracts = session.query(IbContract).filter(IbContract.contract_id == contract.key).all()
    detail = details[-1]
    for contract in contracts:
        contract.ib_contract_id = detail.underConId
    return contracts


def register_executions(session, executions):
    matches = list()

    for contract, execution in executions:
        match = session.query(IbExecution).filter(IbExecution.exec_id == execution.execId).one_or_none()
        if not match:
            match = IbExecution()
            match.exec_id = execution.execId
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
        matches.append(match)





def register_ib_contract_id(session, contract_id, ib_contract_id):
    legs = session.query(IbTradeLeg).filter(and_(IbTradeLeg.contract_id == contract_id,
                                                 IbTradeLeg.ib_contract_id == None)).all()
    for leg in legs:
        leg.ib_contract_id = ib_contract_id

    if legs:
        session.commit()


def register_ib_price(session, contract, price_data):
    p = IbPrice()
    p.contract_id = contract.key
    p.bid = price_data['bid']
    p.ask = price_data['ask']
    p.price = price_data['mid']
    p.time = price_data['bid_time']
    session.add(p)


def register_order(session, trade, action, qty, status=OrderStatus.READY):
    o = IbOrder()
    o.u_id = trade.u_id
    o.action = action
    o.qty = qty
    o.status = status
    o.trade_id = trade.id
    o.contract_id = trade.contract_id
    session.add(o)
    session.commit()


def register_trade(session, trade):
    if hasattr(trade, '__iter__'):
        trade = Trade.from_gsheet_row(trade)
    return _register_trade_from_trade_obj(session, trade)


def request_ib_contract_ids(session, ib_app):
    """Runs process to attach ib_contract_id to each BAG contract."""
    sql = """SELECT contract_id 
             FROM ib_contracts 
             WHERE parent_contract_id IN (SELECT contract_id FROM ib_contracts WHERE sec_type = 'BAG')
             AND ib_contract_id IS NULL"""

    contract_ids = session.execute(sql).fetchall()
    if not contract_ids:
        return
    ids = [c.contract_id for c in contract_ids]
    contracts = session.query(IbContract).filter(IbContract.contract_id.in_(ids)).all()
    callback = functools.partialmethod(register_ib_contract_id, session)

    for db_contract in contracts:
        ib_app.request_contract_id(db_contract.ib_contract, callback=callback)


def run_ib_database(ib_app, Session):
    """Main method orchestrates ibdb functions to manage trade executions with Interactive Brokers."""
    while True:
        session = Session()

        sync_gsheet_trades(session)
        request_ib_contract_ids(session, ib_app)
        sync_price_subscriptions(session, ib_app)
        evaluate_trades(session)
        place_orders(session, ib_app)
        delete_old_prices(session)

        session.commit()
        session.close()

        sleep(EVAL_INTERVAL)


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


def sync_price_subscriptions(session, ib_app):
    """Checks missing price subscriptions on open IbTrades and attempts to subscribe."""
    current_prices = session.query(IbPrice).filter(
        IbPrice.time > datetime.utcnow() - timedelta(seconds=60)).all()
    current_contract_ids = list(set([p.contract_id for p in current_prices]))

    stocks_missing = session.query(IbTrade).filter(
        and_(IbTrade.underlying_contract_id.notin_(current_contract_ids),
             IbTrade.date_exited == None)
    ).all()
    others_missing = session.query(IbTrade).filter(
        and_(IbTrade.contract_id.notin_(current_contract_ids),
             IbTrade.date_exited == None,
             IbTrade.sec_type != 'BAG')
    ).all()
    all_missing = list(stocks_missing) + list(others_missing)
    u_ids = list()

    for trade in all_missing:
        if trade.u_id in u_ids:
            continue
        ib_app.register_trade(trade.gsheet_trade)


def _get_price_data_import(contract, data):
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

    mid = round((bid+ask)/2, 2)
    data['mid'] = mid
    data['mid_time'] = check_time

    return data


def _register_trade_leg(trade, leg, sequence):
    o = IbTradeLeg()
    o.ratio = leg['ratio']
    o.action = leg['action']
    o.expiration = int('{}{}{}'.format(leg['year'], leg['month'], leg['day']))
    o.symbol = leg['symbol']
    o.strike = leg['strike']
    o.sequence = sequence
    o.contract_id = leg['contract'].key
    o.trade_u_id = trade.u_id
    trade.legs.append(o)


def _register_trade_from_trade_obj(session, trade):
    if not trade.tactic_parsed:
        trade.parse_tactic()

    if trade.u_id:
        match = get_trade_by_uid(session, trade.u_id)
        if match:
            maybe_update_trade(match, trade)
            session.commit()
            return match

    t = IbTrade()
    _set_sql_trade_from_gsheet_trade(t, trade)
    session.add(t)
    session.commit()

    for i, leg in enumerate(trade.leg_data, start=1):
        _register_trade_leg(t, leg, i)
        if i == len(trade.leg_data):
            session.commit()


def _set_sql_trade_from_gsheet_trade(sql_trade, sheet_trade):
    t, trade = sql_trade, sheet_trade

    t.alert_category = trade.alert_category
    t.date_entered = trade.date_entered
    t.date_exited = trade.date_exited
    t.pct_sold = trade.pct_sold
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
    t.underlying_contract_id = sheet_trade.get_stock_contract().key
    t.contract_id = sheet_trade.get_contract().key



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
        call_with_session(register_contract_details, contract, details)

    @iswrapper
    def execDetails(self, req_id: int, contract, execution):
        self.executions[req_id].append((contract, execution))

    @iswrapper
    def execDetailsEnd(self, req_id: int):
        executions = self.executions.pop(req_id)
        call_with_session(register_executions, executions)











