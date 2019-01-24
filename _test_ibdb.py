import os
from collections import namedtuple
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import ibdb as db
from ibdb import IbTrade, IbTradeLeg, IbOrder, IbPrice, IbContract, IbExecution

test_engine = create_engine('sqlite:///' + os.path.join(os.path.dirname(__file__), 'data', 'ib_test.db'))
Session = sessionmaker(bind=test_engine)
session = Session()


def _load_fake_prices(session, sheet_trades):
    valid = 0
    has_target = False
    has_stop = False

    for trade in sheet_trades:
        if not trade.u_id:
            continue

        u_price = trade.underlying_entry_price
        e_price = trade.entry_price
        s1 = trade.stop_price1
        t1 = trade.target_price1
        contract = trade.get_stock_contract()
        if not all((u_price, e_price, s1, contract)):
            continue

        if valid % 2 == 0 and trade.is_short:
            multiplier = 0.97 if trade.is_short else 1.02
            has_target = True
            price1 = t1
        else:
            multiplier = 0.97 if trade.is_long else 1.02
            has_stop = True
            price1 = s1

        p1 = IbPrice()
        p1.price = round(price1*multiplier, 2)
        p1.contract_id = contract.key
        p1.time = datetime.utcnow() + timedelta(seconds=10)
        session.add(p1)

        if trade.sec_type != 'STK':
            contract = trade.get_contract()
            p2 = IbPrice()
            p2.contract_id = contract.key
            p2.price = round(e_price*multiplier, 2)
            p2.time = datetime.utcnow() + timedelta(seconds=15)
            session.add(p2)

        valid += 1

        if valid >= 3 and has_target and has_stop:
            break
    session.commit()


def _do_fake_executions(session):
    orders = session.query(IbOrder).filter(IbOrder.status == db.OrderStatus.READY).all()
    for order in orders:
        if not order.trade.entry_price:
            continue
        e = IbExecution()
        if order.action == 'BUY':
            e.side = 'BOT'
            e.price = e.avg_price = order.trade.original_entry_price
        else:
            e.side = 'SLD'
            e.price = e.avg_price = db.get_price_by_contract_id(
                session, order.contract_id, min_seconds=60
            )
            e.order_id = order.request_id

        e.exec_id = str(db.utils.get_uid())
        e.exchange = 'SMART'
        e.shares = order.trade.total_qty

        session.add(e)
    if orders:
        session.commit()


class FakeIbApp:
    def __init__(self, session):
        self.session = session

    def placeOrder(self, request_id, contract, ib_order):
        print("FakeIbApp: placeOrder called {}".format(ib_order))

    def request_executions(self):
        print("FakeIbApp: request_executions called")
        _do_fake_executions(self.session)

    def request_contract_id(self, contract):
        print("FakeIbApp: request_contract_id called")
        details = namedtuple('details', ['underConId'])
        details.underConId = db.utils.get_uid()
        db.register_contract_details(self.session, contract, details)
        self.session.commit()

    def register_trade(self, trade, force=False):
        print("FakeIbApp: register_trade called: {}".format(trade))

    def get_next_id(self):
        return db.utils.get_uid()



def setup():
    db.Base.metadata.drop_all(bind=test_engine)
    db.Base.metadata.create_all(bind=test_engine)
    for cls in (IbPrice, IbExecution, IbOrder, IbContract, IbTradeLeg, IbTrade):
        print("Clearing {}".format(cls.__name__))
        session.query(cls).delete(synchronize_session=False)
    session.commit()

    callbacks = [(_load_fake_prices, (session,))]
    db.sync_gsheet_trades(session, trade_callbacks=callbacks)

    ib_app = FakeIbApp(session)
    db.evaluate_trades(session)
    _do_fake_executions(session)
    db.sync_fills(session)


    trades = session.query(IbTrade).filter(IbTrade.date_exited == None).all()
    print(trades)




def run_tests():
    setup()

if __name__ == '__main__':
    run_tests()