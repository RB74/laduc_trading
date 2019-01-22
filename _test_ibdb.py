import os
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import ibdb as db
from ibdb import IbTrade, IbTradeLeg, IbOrder, IbPrice

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
            price = t1
        else:
            multiplier = 0.97 if trade.is_long else 1.02
            has_stop = True
            price = s1

        p1 = IbPrice()
        p1.price = round(price*multiplier, 2)
        p1.contract_id = contract.key
        p1.time = datetime.utcnow() + timedelta(seconds=10)
        session.add(p1)
        valid += 1

        if valid >= 3 and has_target and has_stop:
            break



def setup():
    db.Base.metadata.create_all(bind=test_engine)
    for cls in (IbPrice, IbOrder, IbTradeLeg, IbTrade):
        session.query(cls).delete(synchronize_session=False)
    session.commit()
    callbacks = [(_load_fake_prices, (session,))]
    db.sync_gsheet_trades(session, trade_callbacks=callbacks)


    trades = session.query(IbTrade).filter(IbTrade.date_exited == None).all()
    print(trades)




def run_tests():
    setup()

if __name__ == '__main__':
    run_tests()