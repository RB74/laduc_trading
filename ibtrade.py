import os
import utils
import logging
import gspread
from cachetools import TTLCache
from datetime import datetime, timedelta
from ibutils import (get_stock_contract, get_option_contract,
                     get_bag_contract, send_closing_trade_notification)
from oauth2client.service_account import ServiceAccountCredentials

log = logging.getLogger(__name__)
config = utils.config

STOP_LOSS = 'Stop loss'
TGT_REACHED = 'Target reached'

SHEET_TEST_MODE = config['ib'].getboolean('sheet_test_mode', True)
PRODUCTION_SHEET_ID = '1p8rr5tmroFuKNyko40jYJmK7PwEGIVHCkPxlW446LIk'
TEST_SHEET_ID = '1aBxtmUXH2miPi8DigvPEz9kg6kRuTXzhkD61gV9ZLC4'
MAP_10_SEC = TTLCache(100*100, 10)
MAP_30_SEC = TTLCache(100*100, 10)
MAP_30_MIN = TTLCache(100*100, 30*60)

if SHEET_TEST_MODE:
    settings_filename = 'ib_cfg_z.json'
    GSHEET_ID = TEST_SHEET_ID
else:
    settings_filename = 'ib_cfg.json'
    GSHEET_ID = PRODUCTION_SHEET_ID


def get_data_entry_sheet():
    """
    Returns the google sheet on a 30-minute cache.
    :return:
    """
    try:
        return MAP_30_MIN['DE_SHEET']
    except KeyError:
        for i in range(3):
            try:
                credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    os.path.join(os.path.dirname(__file__), 'service-credentials.json'),
                    ['https://spreadsheets.google.com/feeds'])
                MAP_30_MIN['DE_SHEET'] = sheet = gspread.authorize(credentials)\
                    .open_by_key(GSHEET_ID)\
                    .worksheet('DataEntry')
                MAP_10_SEC.pop('DE_SHEET_VALUES', None)
                return sheet

            except gspread.exceptions.APIError:
                if i == 2:
                    raise


def now_is_rth():
    try:
        return MAP_30_SEC['OUTSIDE_RTH']
    except KeyError:
        MAP_30_SEC['OUTSIDE_RTH'] = o = utils.now_is_rth()
        return o


def get_data_entry_rows():
    try:
        return MAP_10_SEC['DE_SHEET_VALUES']
    except KeyError:
        MAP_10_SEC['DE_SHEET_VALUES'] = values = get_data_entry_sheet().get_all_values()[1:]
        return values


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


class TradeSheet:
    def __init__(self, ib_app=None, refresh_seconds=45):
        if ib_app is None:
            from ib import IbAppThreaded
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
                or not self._last_updated\
                or force:
            log.debug("Syncing trades.")
        else:
            return self._trades

        self._last_updated = datetime.now()
        rows = get_data_entry_rows()
        current_trades = get_data_entry_trades(self, rows)
        #log.debug("{} current trades".format(len(current_trades)))
        current_keys = [t.key for t in current_trades]

        # Add new current trades
        for t in current_trades:
            key = t.key
            if key in self._closed:
                continue
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
                existing_trade = self._trades[key]
                existing_trade.update_from_trade(t)
                #log.debug("Updating existing trade: {}".format(key))
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
        if not rows:
            rows = get_data_entry_rows()

        partials = [Trade.from_gsheet_row(row, self) for row in rows
                    if 0 < utils.ensure_int_from_pct(row[9]) < 100]
        updated = []
        for trade in self._trades.values():
            matches = [p for p in partials if p.key == trade.key]
            if len(trade.partial_exits) == len(matches):
                continue
            elif trade.partial_exits:
                trade.partial_exits.clear()
            trade.partial_exits.extend(matches)
            updated.append(trade.key)
        if updated:
            log.debug("Syncing partial exits: OK ({}): "
                      "{}.".format(len(updated), updated))


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
        'orders', '__locked', '__tactic_parsed', '__direction_determined', 'last_execution',
        'fail_count'
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
        self.__tactic_parsed = False
        self.__direction_determined = False
        self.last_execution = None
        self.fail_count = 0

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
    def tactic_parsed(self):
        return self.__tactic_parsed

    @property
    def target_reached(self):
        return self.close_reason == TGT_REACHED

    def get_target_size(self):
        partial = self._calc_portion_size(self.number_of_targets or 1)
        open = self.size_open
        return open if abs(partial) >= abs(open) else partial

    def get_stop_size(self):
        partial = self._calc_portion_size(self.number_of_stops or 1)
        open = self.size_open
        return open if abs(partial) >= abs(open) else partial

    def get_open_size(self):
        return self._calc_portion_size(1)

    def get_open_action(self):
        if self.sec_type == 'BAG':
            # TODO: When would we sell/short a bag?
            return 'BUY'
        return 'SELL' if self.size < 0 else 'BUY'

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

    @staticmethod
    def from_ib_trade(ib_trade):
        t = Trade()
        for key in t.__slots__:
            value = getattr(ib_trade, key, None)
            if value is not None:
                setattr(t, key, value)
        t.parse_tactic()
        t._determine_direction()
        return t

    def close(self, price, timestamp, qty=None, validate=True, email_only=False):
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

        if self.is_short and price > 0:
            price = -price

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
                new_uid = utils.get_uid()
                # 1 index = 1 (not 0 like Python)
                sheet.update_cell(row, 10, '{}%'.format(close_pct))  # % Sold
                sheet.update_cell(row, 11, price)                    # Exit Price
                sheet.update_cell(row, 13, timestamp)                # Date Exited
                sheet.update_cell(row, 14, self.close_reason)        # Notes
                sheet.update_cell(row, 22, new_uid)                  # UID

                self.exit_price = abs(price)
                self.date_exited = utils.to_timestamp(timestamp)
                self.u_id = new_uid
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
        self.exit_price = abs(price)
        self.date_exited = utils.to_timestamp(timestamp)
        self.u_id = values[21]

        # Register partial trade
        trade = Trade.from_gsheet_row([str(v) for v in values], trade_sheet=self.sheet)
        self.partial_exits.append(trade)

    def get_partial_close_decision(self, qty=None):
        """
        Determines whether or not the trade should be closed as a partial sale or closed entirely.
        :return: (tuple[bool, float])
            bool: True = yes partial sale, False = no partial sale.
            float: The closing percentage as an integer (between 0 - 100)
        """
        if qty:
            # We should determine the exact close
            # percentage based on qty sold.
            close_pct = round((self.get_open_size()/abs(qty))*100, 2)
            is_partial_sale = close_pct < 100
            return is_partial_sale, close_pct

        num_exits = len(self.partial_exits) + 1
        if self.stopped and num_exits < self.number_of_stops > 1:
            is_partial_sale = True
            close_pct = round(100/self.number_of_stops, 0)
        elif self.target_reached and num_exits < self.number_of_targets > 1:
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

        self._bag_contract = get_bag_contract(self.leg_data, getattr(self.sheet, 'app', None))
        return self._bag_contract

    def get_cash_contract(self):
        if self._cash_contract is None and self.sec_type == 'CASH':
            c = get_stock_contract(self.symbol + 'USD', self.exchange)
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
        self.__tactic_parsed = True

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
        self.entry_price = price
        sheet = get_data_entry_sheet()
        try:
            match = sheet.findall(self.u_id)[0]
        except IndexError:
            # uid was removed from the record.
            # Invalidate the trade.
            self.valid = False
            log.debug("UID was removed from trade - invalidating: {}".format(self))
            return

        # Short prices are negative in GSheet.
        if self.is_short and price > 0:
            price = -price

        sheet.update_cell(match.row, 9, price)
        self.highlight_cell(1, bg_color='white')
        self.highlight_cell(9, bg_color='white')

    def update_from_trade(self, trade):

        if self.locked or trade.locked:
            log.debug("Rejecting update on locked trade: {}".format(self))
            return False

        if not trade.__tactic_parsed:
            trade.parse_tactic()
        if not trade.__direction_determined:
            trade._determine_direction()

        if trade.alert_category != self.alert_category and self.alert_category:
            if 'correction' in trade.alert_category.lower():
                # TODO: Process correction.
                # Maybe means cancel order/restart. Need to be careful
                # to cancel and prevent the GSheet from updating.
                pass

        self.alert_category = trade.alert_category
        self.underlying_entry_price = trade.underlying_entry_price
        self.row_idx = trade.row_idx

        self.stop_price = trade.stop_price
        self.stop_price1 = trade.stop_price1
        self.stop_price2 = trade.stop_price2

        self.target_price = trade.target_price
        self.target_price1 = trade.target_price1
        self.target_price2 = trade.target_price2
        if trade.date_exited:
            self.date_exited = trade.date_exited
        if trade.pct_sold:
            self.pct_sold = trade.pct_sold
        if trade.exit_price:
            self.exit_price = trade.exit_price
        if not self.u_id and trade.u_id:
            self.u_id = trade.u_id


        if trade.partial_exits:
            self.partial_exits = trade.partial_exits

        self._determine_direction()

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
        self.__direction_determined = True

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
        try:
            price = float(price)
        except:
            price = None
        if not price:
            log.error("Invalid price: {} {}".format(price, self))
            return False
        if not self.u_id or len(self.u_id) < 3:
            log.error("Failed to close trade (invalid UID) {}".format(self))
            return False
        elif self.date_exited:
            log.error("Failed to close trade (pre-existing date exited {}): {}".format(self.date_exited, self))
            return False
        elif self.exit_price:
            log.error("Failed to close trade (pre-existing exit price {}): {}".format(self, self.exit_price))
        if not isinstance(timestamp, str):
            try:
                timestamp = timestamp.strftime('%m/%d/%Y %H:%M')
            except AttributeError:
                log.error("Failed to close trade (invalid timestamp {}): {}".format(timestamp, self))
                return False

        return price, timestamp

    def _calc_portion_size(self, number):
        """
        Gives positive value of a portion of capital allocated
        to a trade based on a given number.
        capital is defined as abs(Trade.size)*1000
        :param number: (numeric)
            The number of portions to break trade capital into.
        :return: total_position_size / number
        """
        number = abs(number) if number else 1
        size = abs(self.size)

        # Figure out entry price.
        if not self.entry_price:
            entry_price = self.sheet.app.get_midpoint_by_symbol(
                self.symbol,
                self.sec_type.lower(),
                contract_key=self.get_contract().key)
            if entry_price is None:
                return 1
        else:
            entry_price = self.entry_price

        # GSheet size: 1 = $1000
        capital = size * 1000
        # Options price is based on 100 contracts.
        price_per = abs(entry_price*100 if self.sec_type in ('OPT', 'BAG') else entry_price)

        # Total number of shares available
        quantity = capital / price_per
        # Amount of shares per portion.
        portion_size = round(quantity / number, 0)
        log.debug("{}: Portion size: {} from number "
                  "{}".format(self.key, portion_size, number))
        return portion_size if portion_size >= 1 else 1

    def _add_qtys(self, *numbers):
        qty = 0
        for n in numbers:
            if n is not None:
                qty += 1
        return qty

    def __lt__(self, other):
        return self.date_entered < other.date_entered

    def __repr__(self):
        side = 'Short' if self.is_short else 'Long'
        return "{}{}Trade(symbol='{}', entry_date='{}', und_entry='{}', " \
               "stop_price='{}', target='{}', u_id='{}', cont_id='{}')".format(
                side, str(self.sec_type).title(), self.symbol,
                self.date_entered, self.underlying_entry_price,
                self.stop_price, self.target_price, self.u_id,
                getattr(self.get_contract(), 'key', 'N/A'))