"""
The database layer to Interactive Brokers trade integration.
We need to have a few pieces of data stored in one place with persistence.

    1) Trades entries from Google Sheets.
    2) Prices from interactive brokers on open trades.
    3) Orders executed as trade targets are hit.
"""

import os
import utils
from time import sleep
from ibtrade import Trade
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


EVAL_INTERVAL = utils.config['ib'].getinteger('eval_interval', 30)
Base = declarative_base()


class IbPrice(Base):
    __tablename__ = 'ib_prices'

    contract_id = Column(String, nullable=False, primary_key=True)
    time = Column(DateTime, nullable=False, primary_key=True)
    price = Column(Float, nullable=False, primary_key=True)
    bid = Column(Float)
    ask = Column(Float)


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


class IbTradeLeg(Base):
    __tablename__ = 'ib_trade_legs'

    trade_id = Column(Integer, ForeignKey('ib_trades.id'))
    symbol = Column(String)
    action = Column(String)
    ratio = Column(Integer)
    sequence = Column(Integer)
    contract_id = Column(String)
    ib_contract_id = Column(Integer)
    expiration = Column(Integer)
    date_added = Column(DateTime, default=datetime.utcnow)
    trade = relationship("IbTrade", foreign_keys=['ib_trades.id'])


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
    contract_id = Column(String)
    underlying_contract_id = Column(String)
    date_added = Column(DateTime, default=datetime.utcnow)

    legs = relationship("IbTradeLeg")
    orders = relationship("IbOrder")

    @hybrid_property
    def profits_down(self):
        return self.target_price1 < self.underlying_entry_price

    @hybrid_property
    def profits_up(self):
        return self.target_price1 > self.underlying_entry_price

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


DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

DB_PATH = os.path.join(DATA_DIR, 'ib.db')

engine = create_engine("sqlite3:///" + DB_PATH)
Session = scoped_session(sessionmaker(bind=engine))


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
    return session.query(IbTrade).filter(IbTrade.u_id == str(uid)).one()


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
    from ibtrade import get_data_entry_trades
    trades = {str(t.u_id): t for t in get_data_entry_trades()}

    for order in orders:
        sheet_trade = trades.get(order.u_id, None)
        contract = sheet_trade.get_contract()

        ib_order = Order()
        ib_order.action = order.action
        ib_order.orderType = 'MKT'
        ib_order.totalQuantity = order.qty

        order.request_id = ib_app.get_next_id()
        order.status = OrderStatus.PLACED
        session.commit()

        ib_app.placeOrder(order.request_id, contract, ib_order)


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


def run_ib_database(ib_app):

    while True:
        session = Session()

        sync_gsheet_trades(session)
        evaluate_trades(session)
        place_orders(session, ib_app)
        delete_old_prices(session)

        session.commit()
        session.close()

        sleep(EVAL_INTERVAL)


def sync_gsheet_trades(session):
    from ibtrade import get_data_entry_trades
    trades = get_data_entry_trades()
    for trade in trades:
        register_trade(session, trade)


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
    if not trade.__tactic_parsed:
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



