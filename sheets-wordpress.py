import os
import sys
import time
import gspread
import requests
import configparser
import multiprocessing
from utils import try_float, get_cumulative_price
from datetime import datetime
from Laduc_SQL import SQLClient
from Laduc_WordPress import WordPressClientInit as WPInit
from oauth2client.service_account import ServiceAccountCredentials


wp_category_map = {
    'chase': 446,
    'idea': 447,
    'lotto': 448,
    'advancedoptions': 449,
    'quant': 450,
    'swing': 451,
    'trend': 452,
}

start_time = datetime.now()


def _get_wp_category(entry_type) -> int:
    """
    Retrieves the appropriate wordpress category ID
    for creating new posts. This allows different post categories
    based on alert type.
    :param entry_type:
    :return:
    """
    if entry_type == 'alert-test':
        return 301
    t_parts = str(entry_type).split('-')
    t = (t_parts[-1] if len(t_parts) > 1 else entry_type).lower()
    return wp_category_map.get(t, 289)


def _get_stop_change(g_row, sql_rows) -> bool:
    """
    2/9/2019: Trigger a Trade Alert when the stop_loss price(s) have changed.

    Gets the sum of stop_loss on GSheet and SQL rows.

    If stop_loss has changed
      - the post title is updated to include "STOPCHANGED"
      - the post is allowed to trigger (even if it's already triggered).

    :returns:
        False when the trade u_id doesn't exist in the DB or stop_loss has NO CHANGE.
        'STOPCHANGED' when the stop_loss has changed.
    """
    try:
        sql_row = sql_rows[g_row['u_id']]
    except KeyError:
        return False

    if g_row['fields']['date_exited'].strip():
        return False

    g_stop = get_cumulative_price(g_row['fields']['stop_loss'])
    sql_stop = get_cumulative_price(sql_row['stop_loss'])

    if g_stop == sql_stop:
        return False

    # Ensure trigger value is in the WP Post title.
    v = 'STOPCHANGED'
    if v not in g_row['post']['title']:
        g_row['post']['title'] = g_row['post']['title'].replace(
            'Trade Alert - #', 'Trade Alert - ({}) #'.format(v))

    return True


def main():
    """
    The main method to execute all the operations related to creating posts  
    Parameters : None
    
    Return values: None
    """

    global SQL, test_mode, sheet_test_mode, test_wordpress    
    print(start_time)

    set_globals()            # Reads config.ini
    SQL = SQLClient()        # Database connection
    test_mode = False        # False == Production
    test_wordpress = False   # False == Production
    sheets = GSC()               # GoogleSheets connection

    try:
        # Fetching the data from Google Spreadsheet
        # g_data= list of rows from worksheet 'DataEntry' of our Google spreadsheet
        # sheet_test_mode= mode for worksheet of our Google spreadsheet
        print("Reading/formatting GSheet.")
        g_data, sheet_test_mode = sheets.get_sheet_and_format()
    except Exception as e:
        print('Issue getting g_data -', repr(e))
        try:
            # If any exception occurs then closing connection to SQL Database
            SQL.close()
        except:
            pass
        finally:
            sys.exit(0)

    valid_ids = get_uids_valid_for_post(SQL, g_data)

    if not valid_ids:
        print('No valid u_ids')
        try:
            SQL.close()
        except:
            pass
        return

    print(len(valid_ids), ' valid entries to check')
    if sheet_test_mode:
        print('No Trade Entries to be processed as Trade Entry Sheet Test Mode:', sheet_test_mode)

    updated = 0
    wp = WordPressClient(WPInit())

    for obj in g_data:
        if obj['u_id'] not in valid_ids:
            continue

        # Checking for global variables 'sheet_test_mode','test_mode' and 'test_wordpress'
        # If test_mode is True, don't do anything and just return True
        if sheet_test_mode or (test_mode and not test_wordpress):
            success = False
            # 7/23/2018: Add the trade entry to MySQL as finished to
            # permanently invalidate the u_id.
            try:
                o = obj['fields']
                o['finished'] = 1
                print("Updating trade entry in database.")
                SQL.update_trade_entries([o])
                updated += 1
            except Exception as e:
                print('Updating Entries Issue -', repr(e))
        else:
            print("Creating WP Post for UID: {}".format(obj['u_id']))
            success = wp.post_create(obj['post'], obj['u_id'])

        # Checking for success variable
        if not success:
            continue

        try:
            print('Updating', obj)
            # Updating all trade entries in SQL
            SQL.update_trade_entries([obj['fields']])
            updated += 1
        except Exception as e:
            print('Updating Entries Issue -', repr(e))
        # Comparing for global variable of 'sheet_test_mode' and 'test_mode'
        # If Production, publish the drafts posts
        if not sheet_test_mode and not test_mode:
            try:
                tries = 3
                while tries > 0:
                    # Requesting publish end-point to publish the draft posts
                    r = requests.get('https://laductrading.com/trade-alerts-publish/', headers=wp.headers)
                    if r.status_code in [200]:
                        print('Successfully hit publish endpoint')
                        break
                    else:
                        tries -= 1
                        time.sleep(1)
                        print('Trade Alerts Publish status -', r.status_code, r.reason)
                        with open('trade-alert-publish-error.html', 'wb') as f:
                            f.write(r.content)
                    if tries <= 0:
                        print('Trade Alerts Publish Out of Tries')
                        break
            except Exception as e:
                print('Trade Alerts Publish endpoint issue -', repr(e))

    print('Successfully Updated', updated, 'rows')
    # It is closing connection with SQL
    SQL.close()

    print('------')
    print()


def get_uids_valid_for_post(db_client, g_data, compare_prices=False):
    """
    Comparing the Google Spreadsheet data against database data to find out valid data to create posts
    :param db_client: (Laduc_SQL.SQLClient)
    :param g_data: (list(dict)) A list of rows from Google Spreadsheet
    :param compare_prices: (boolean, default False)
        False will return new u_ids that aren't in the database
            AND u_ids where the target/stop price has changed.

        True will include the above AND u_ids if the entry or exit
        price has changed.

    returns: (list)
        Of u_ids that need a new Wordpress Post.
    """
    u_ids = list(set([x['u_id'] for x in g_data]))

    # Get database trade entries
    try:
        # Unfinished - Open Trades
        finished = False
        sql_check = {
            row['u_id']: {
                'entry_price': row['entry_price'],
                'exit_price': row['exit_price'],
                'stop_loss': row['stop_loss'],
                'profit_exit': row['profit_exit']
            }
            for row in db_client.get_trade_entries(u_ids, finished)
        }

        # Completed Trades
        finished = True
        sql_finished = {obj['u_id'] for obj in db_client.get_trade_entries(u_ids, finished)}
    except Exception as e:
        print('Check - SQL_data issue -', repr(e))
        return list()

    # Maybe return only new u_ids (and stop/target changed u_ids) w/ a date_entered value.
    if not compare_prices:
        return [
            g_row['u_id'] for g_row in g_data
            if g_row['u_id'] not in sql_finished
            and (g_row['u_id'] not in sql_check
                 or _get_stop_change(g_row, sql_check))
            and g_row['fields']['date_entered'].strip()
        ]

    # Do price comparison
    valids = set()

    # Get only unfinished trades with a date_entered value.
    g_open = [g_row for g_row in g_data
              if g_row['u_id'] not in sql_finished
              and g_row['fields']['date_entered'].strip()]

    for g_row in g_open:
        id = g_row['u_id']

        # Check entry/exit price for change
        if id in sql_check:
            row_entry = "{0:.2f}".format(g_row['fields']['entry_price'])
            row_exit = "{0:.2f}".format(g_row['fields']['exit_price'])
            sql_entry = "{0:.2f}".format(sql_check[id]['entry_price'])
            sql_exit = "{0:.2f}".format(sql_check[id]['exit_price'])

            if row_entry != sql_entry or row_exit != sql_exit:
                # Updated trade entry
                valids.add(id)
            elif _get_stop_change(g_row, sql_check):
                valids.add(id)
        else:
            # New trade entry
            valids.add(id)

    print('Finished', len(g_data) - len(g_open),    '/', len(g_data))
    print('Checked',  len(g_open),                  '/', len(g_data))
    print('Valids',   len(valids),                  '/', len(g_data))

    return list(valids)


class GSC:
    """
    GSC is used for fetching & formatting the data from Google Spreadsheet
    """
    def __init__(self):
        """
        Constructor which is assigning properties 
        """
        #gc  -- It is setting authorized Google Spreadsheet object 
        self.gc = self._authorize()

    def _authorize(self):
        """
        Creating authorized Google Spreadsheet object 
        Function arguments: None
        
        Return values: it is returning authorized Google Spreadsheet object
        """
        # Setting scope variable to  https://spreadsheets.google.com/feeds path
        scope = ['https://spreadsheets.google.com/feeds']
        # it is getting credentials from local service-credentials.json file 
        credentials = ServiceAccountCredentials.from_json_keyfile_name(real_path+sep+'service-credentials.json', scope)
        # Using gspread to get authorization for given sheet with given credentials
        gc = gspread.authorize(credentials)
        # Returning the Google spreadsheet object
        return gc

    def get_sheet_and_format(self):
        """
        Fetching data Google spreadsheet and formatting data for later usage
        Function arguments: None
        
        Return values: 
        g_data= list of rows from Google Spreadsheet
        sheet_test_mode = Test Mode specified in Spreadsheet
        """
        # it is opening Google Spreadsheet using its key  1p8rr5tmroFuKNyko40jYJmK7PwEGIVHCkPxlW446LIk
        sheet = self.gc.open_by_key('1p8rr5tmroFuKNyko40jYJmK7PwEGIVHCkPxlW446LIk')
        # Spreadsheet: <Spreadsheet 'LaDucTrading DataEntry Trading Log (restricted use)' id:1p8rr5tmroFuKNyko40jYJmK7PwEGIVHCkPxlW446LIk>
        # it is assigning 'DataEntry' worksheet to ws variable 
        ws = sheet.worksheet("DataEntry")
        # it is assigning all rows of worksheet to g_rows
        g_rows = ws.get_all_values()

        global g_header
        g_header = [x.strip().replace('\n', ' ') for x in g_rows[0] if x.strip()]
        # g_header: ['TYPE', 'SYMBOL', 'POSITION SIZE ($1000)', 'TACTIC: S or O', 'THESIS', 'E= STOCK UNDERLYING ENTRY PRICE', 'S= STOCK UNDERLYING STOP LOSS', 'P= STOCK UNDERLYING PROFIT EXIT', 'ENTRY PRICE', '% SOLD', 'EXIT PRICE', 'DATE ENTERED', 'DATE EXITED', 'NOTES', '% PROFIT /LOSS', 'REALIZED PROFIT /LOSS', 'STATUS', 'DAYS IN TRADE', 'MONTH', 'WEEK ENDING', 'ROW', 'UID', 'TEST_MODE', 'FALSE']
        g_data = []
        # it is setting test mode to sheet_test_mode
        mode = g_header[-1]
        sheet_test_mode = True if mode == 'TRUE' else False

        # Skipping header and assigning it to grow variable
        #iterating all rows of Spreadsheet data
        for x, row in enumerate(g_rows[1:]):
            #Checking if 'DATE ENTERED', 'ENTRY PRICE' & 'UID' have values and 'ENTRY PRICE' also has decimal
            if row[11].strip() and row[8] and '.' in row[8] and row[21].strip():

                # skip row if '% SOLD' not specified & 'EXIT PRICE' is specified
                if row[10] and not row[9]:
                    continue

                # Skip missing 'ENTRY PRICE'
                if not row[8]:
                    continue

                # skip row if 'ENTRY PRICE' and '% SOLD' have values but 'EXIT PRICE' does not have
                if row[8] and row[9] and not row[10]:
                    continue

                # if 'EXIT PRICE' has value but trade hasn't exited yet ('DATE EXITED'), skip
                if row[10] and not row[12]:
                    continue

                # skip row if '% SOLD' has value but 'EXIT PRICE' does not have
                if row[9] and not row[10]:
                    continue

                obj = {}
                # g_header: ['TYPE', 'SYMBOL', 'POSITION SIZE ($1000)', 'TACTIC: S or O', 'THESIS', 'E= STOCK UNDERLYING ENTRY PRICE', 'S= STOCK UNDERLYING STOP LOSS', 'P= STOCK UNDERLYING PROFIT EXIT', 'ENTRY PRICE', '% SOLD', 'EXIT PRICE', 'DATE ENTERED', 'DATE EXITED', 'NOTES', '% PROFIT /LOSS', 'REALIZED PROFIT /LOSS', 'STATUS', 'DAYS IN TRADE', 'MONTH', 'WEEK ENDING', 'ROW', 'UID', 'TEST_MODE', 'FALSE']
                #iterating over all headers of Google Spreadsheet
                for x, key in enumerate(g_header):
                    # Pushing values of row list into obj key dictionary
                    obj[key] = row[x]
                # Pushing values of row list into obj key dictionary    
                obj['% SOLD'] = obj['% SOLD'].replace('%','')
                obj['% SOLD'] = obj['% SOLD'] if obj['% SOLD'] else '0.00'
                obj['EXIT PRICE'] = obj['EXIT PRICE'] if obj['EXIT PRICE'] else '$0.00'
                # obj['u_id'] = '|'.join([  obj['TYPE'], obj['SYMBOL'], obj['TACTIC: S or O'].lower().strip().replace(' ',''), obj['% SOLD'], obj['EXIT PRICE']  ])
                obj['u_id'] = str(obj['UID'])
                # If UID is not specified, skip remaining data   
                if not obj['u_id']:
                    continue
                obj['STATE'] = 'CLOSED' if obj['STATUS'] == 'EXITED' else 'OPEN'            #str: OPEN
                obj['state'] = 'Trim' if obj['% SOLD'] not in ['100.00','0.00'] and obj['STATE'] != 'CLOSED' else obj['STATE']      #str: OPEN
                
                # Crating tags using SYMBOL, TYPE & state    
                obj['tags'] = []
                obj['tags'].append(obj['SYMBOL'].strip().upper())
                obj['tags'].append(obj['TYPE'])
                obj['tags'].append(obj['state'])            #<class 'list'>: ['DIA', 'Quant', 'OPEN']

                num_rows = 14 if obj['STATUS'] == 'ENTERED' else 18
                # Specifying details required for Post
                obj['post'] = {}
                obj['post']['title'] = 'Trade Alert - #{type} {symbol} - {state}'.format( type=obj['TYPE'], symbol=obj['SYMBOL'], state=obj['state'] )
                # TODO: Ensure ['post']['categories'] works right --> Swap below lines if there's an error.
                #obj['post']['categories'] = 289 #ID 291 'JOT Test', ID 289 'Trade Alerts'
                obj['post']['categories'] = _get_wp_category(obj['TYPE'])
                obj['post']['author'] = 2                           # Samantha LaDuc
                obj['post']['status'] = 'draft'                     # Creating post in draft mode
                obj['post']['tags'] = obj['tags']
                obj['post']['template'] = 'page-templates/single-no-sidebar.php'
                # obj['post'] : {'title': 'Trade Alert - #Quant DIA - OPEN', 'categories': 289, 'author': 2, 'status': 'draft', 'tags': ['DIA', 'Quant', 'OPEN'],'template': 'single-no-sidebar.php'}

                # Build HTML TA content
                content = ''
                #  it is appending post title in format <table id="trade-alert"><caption>{title}</caption>
                content += '<table id="trade-alert">' \
                           '<caption>{title}</caption>'.format(title=obj['post']['title'])

                content += '<thead><tr>'
                # Iterating over all keys of Google Spreadsheet headers 'g_header'
                for key in g_header[:num_rows]: #'''g_header[:num_rows]: ['TYPE', 'SYMBOL', 'POSITION SIZE ($1000)', 'TACTIC: S or O', 'THESIS', 'E= STOCK UNDERLYING ENTRY ,PRICE', 'S= STOCK UNDERLYING STOP LOSS', 'P= STOCK UNDERLYING PROFIT EXIT', 'ENTRY PRICE', '% SOLD', 'EXIT PRICE', 'DATE ENTERED', 'DATE EXITED', 'NOTES', '% PROFIT /LOSS', 'REALIZED PROFIT /LOSS', 'STATUS', 'DAYS IN TRADE']'''
                    if key == 'STATUS':
                        continue
                    # it is appending keys of g_header in format <th>{key}</th>  
                    content += '<th>{key}</th>'.format(key=key)

                content += '</tr></thead>'
                content += '<tbody><tr>'
                for key in g_header[:num_rows]:
                    if key == 'STATUS':
                        continue

                    item = '<td>' + obj[key] + '</td>'
                    # appending each item key in content
                    content += item
                # it is ending of your content
                content += '</tr></tbody></table>'

                obj['post']['content'] = content
                # field is temporary dictionary for 'obj['field']' 
                fields = {}
                fields['u_id'] = obj['u_id']
                fields['type'] = obj['TYPE']
                fields['symbol'] = obj['SYMBOL']
                fields['position'] = obj['POSITION SIZE ($1000)']
                fields['tactic'] = obj['TACTIC: S or O']
                fields['thesis'] = obj['THESIS']
                fields['underlying_entry_price'] = obj['E= STOCK UNDERLYING ENTRY PRICE']
                fields['stop_loss'] = obj['S= STOCK UNDERLYING STOP LOSS']
                fields['profit_exit'] = obj['P= STOCK UNDERLYING PROFIT EXIT']
                fields['entry_price'] = obj['ENTRY PRICE']
                fields['sold_perc'] = obj['% SOLD']
                fields['exit_price'] = obj['EXIT PRICE']
                fields['date_entered'] = obj['DATE ENTERED']
                fields['date_exited'] = obj['DATE EXITED']
                fields['notes'] = obj['NOTES']
                fields['profit_loss_perc'] = try_float(obj['% PROFIT /LOSS'].replace('%', '')) if obj['% PROFIT /LOSS'] else 0
                fields['profit_loss_gross'] = obj['REALIZED PROFIT /LOSS']
                fields['status'] = obj['STATUS']
                fields['days_in_trade'] = try_float(obj['DAYS IN TRADE']) if obj['DAYS IN TRADE'] else 0
                fields['month'] = obj['MONTH']
                fields['week'] = obj['WEEK ENDING']
                #dict: {'u_id': '1521477457903','profit_loss_perc': 0, 'profit_loss_gross': 0.0, 'status': 'ENTERED', 'type': 'Quant', 'symbol': 'DIA', 'position': '2', 'tactic': 'APR 20 $245C', 'thesis': 'Quant Bounce', 'underlying_entry_price': 269.5, 'stop_loss': '$264.50', 'profit_exit': '$248.50', 'entry_price': 5.1, 'sold_perc': '0.00', 'exit_price': 0.0, 'date_entered': '3/19/2018 14:35', 'date_exited': '', 'notes': '1/4 size starter', , 'days_in_trade': 0, 'month': '', 'week': ''}

                currencies = ['underlying_entry_price', 'entry_price','exit_price', 'profit_loss_gross']
                for c in currencies:        #c: profit_loss_gross
                    if fields[c] and '.' in fields[c]:  #fields[c]: $1.01 and str: $269.50
                        fields[c] = try_float(fields[c].replace('$', '').replace(',', '').strip())
                    else:
                        fields[c] = 0.0

                obj['fields'] = fields
                # in these we are appending object into g_data list

                g_data.append(obj)
        # in these we are returning g_data list and sheet_test_mode
        return g_data, sheet_test_mode


class WordPressClient:
    """
    WordPressClient is used for interaction with WordPress
    """
    def __init__(self, WPInit):
        """
        Constructor which is assigning WordPress properties 
        Function arguments:
        WPInit   : Contains Configuration for connecting to WordPressClient  
        
        Return values: None
        """
        #user     -- username from WPInit for Wordpress 
        self.user = WPInit.user
        #password -- password from WPInit for Wordpress
        self.password = WPInit.password
        #apptoken -- token from WPInit for Wordpress
        self.apptoken = WPInit.apptoken
        #api_url  -- 'https://laductrading.com/wp-json/wp/v2/'
        self.api_url = WPInit.api_url
        #headers  -- It is setting headers from WPInit for Wordpress
        self.headers = WPInit.headers

    def post_create(self, obj, u_id):
        """
        To create WordPress post with provided details 
        Function arguments:
        obj  :  it is getting one object with post field of unit object from g_data
        u_id : it is getting id for that post field of unit object from g_data
        
        
        Return values: True/False
        """
        print('Creating Post', u_id)
        # It is assigning values to tag_ids, id, id_failed
        tag_ids, id, id_failed = [], None, False
        # It is getting tags from SQL
        sql_tags = SQL.get_tags(obj['tags'])
        # Iterating over all tags available in obj
        for tag in obj['tags']:
            id = None
            # Comparing tag with all sql_tags of Sql 
            if tag.lower() not in sql_tags:
                # If it is new tag then create
                id = self.tags_create(tag)
                if id:
                    try:
                        # Creating tag in SQL
                        SQL.create_tags([{'name':tag.lower(), 'id':id}])
                    except Exception as e:
                        print('WP post_create - Issue inserting tag in DB', repr(e))
                else:
                    return False
            else:
                # If it is existing tag then get its id from sql_tags
                id = sql_tags[tag.lower()]
            if id:
                # If id exists then it is appending id into tag_ids list
                tag_ids.append(id)
            else:
                # If id does not exists then assigning id_failed to true
                id_failed = True

        # If id_failed to true then it is returning 'FALSE'
        if id_failed:
            return False

        # Assigning all tags available in tag_ids to obj
        obj['tags'] = ','.join([str(x) for x in list(tag_ids)])

        # Setting number of tries,status and url 
        tries = 3
        status = 0
        url = self.api_url + 'posts'
        while tries > 0:
            try:
                print('Attempting to create post')
                try:
                    # Checking for global variable 'test_wordpress'
                    if not test_wordpress:
                        # Calling the posts URL with parameters for creating posts
                        r = requests.post(url, data=obj, headers=self.headers)
                        # Getting status from URL
                        status = r.status_code
                    if status in [403]:
                        # If status is Forbidden (403) then function is returning 'FALSE'
                        print('Forbidden', r, r.reason)
                        print('------')
                        return False
                    elif status in [200, 201]:
                        # If status is OK then function is returning 'TRUE'
                        print('Successfully created', r.json()['id'], r.json()['title']['raw'])
                        return True
                    else:
                        # Reducing number of tries
                        tries -= 1

                except Exception as e:
                    # If any Exception occurs then Retrying
                    print('Retrying create post -', repr(e),'-', status, obj, r.content)
                    tries -= 1
                    time.sleep(2)

            except Exception as e:
                # If any Exception occurs then Reducing number of tries
                tries -= 3
                print('WP post_create - main request -',repr(e))

            if tries <= 0:
                # If maximum retries occured then returning 'FALSE'
                print("Couldn't create post", status, obj)
                return False

    def tags_create(self, tag):
        """
        To create tags on WordPress 
        Function arguments:
        tag  : it is tag which is to be created
        
        Return values: 
        id : returning id of created tag
        """
        # It is setting url for creating tag
        url = self.api_url + 'tags'
        # Setting number of tries,id 
        tries, id = 3, ''
        while tries > 0:
            try:
                # Calling the tags URL with parameters for creating tag
                r = requests.post(url, data={'name': tag}, headers=self.headers)
                # Getting status from URL
                status = r.status_code
                try:
                    # Decoding response from URL
                    data = r.json()
                    # Getting id field from data 
                    id = data['id']
                    break
                except Exception as e:
                    # If any Exception occurs then decreasing tries
                    tries -= 1
                    try:
                        print('Getting tag from wp', tag)
                        # It is getting tag from 'tags_list' global variable
                        id = self.tags_list(tag)
                        if id:
                            # If id is exists then it is breaking try/catch series
                            break
                        else:
                            # Reducing number of tries
                            tries -= 1
                    except Exception as e:
                        tries -= 1
            except Exception as e:
                # If any Exception occurs in calling URL then decreasing 'tries'
                tries -= 3
                print('WP tags_create', repr(e))
            # checking if maximum tries has completed
            if tries <= 0:
                break

        return id


    def tags_list(self, tag):
        """
        Fetching tag id based on tag name from WordPress 
        Function arguments:
        tag  : tag name which to be searched
        
        Return values: 
        id : returning id of matching tag
        """
        obj = {}
        if tag:
            obj['search'] = tag
        url = self.api_url + 'tags'
        tries = 3
        while tries > 0:
            print(datetime.now(), 'trying to list tags')
            try:
                # Requesting WordPress to fetch the mathcing tag
                r = requests.get(url, params=obj, headers=self.headers)
                status = r.status_code
                if status in [200]:
                    data = r.json()
                    # Retrieving tag id from the response data
                    for x, row in enumerate(data):
                        if row['name'].lower().strip() == tag.strip().lower():
                            return data[x]['id']
                    return None
                else:
                    tries -= 1
            except Exception as e:
                tries -= 3
                print('WP tags_list -', repr(e))

            if tries <= 0:
                return None

        return None


def set_globals():    
    """
    Setting up global variables from config.ini file
    Function arguments: None
     
    Return values: None
    """
    global sep, real_path, config, config_path
    real_path = os.path.dirname(os.path.realpath(__file__))
    sep = os.path.sep
    config_path = real_path+sep+'config.ini'
    config = configparser.ConfigParser()
    config.read(config_path)


if __name__ == '__main__':
    p = multiprocessing.Process(target=main)
    p.start()
    p.join(50)
    if p.is_alive():
        print("Timeout: 50 seconds no completion.")
        print('----------------------------------')
        # Terminate
        p.terminate()
        p.join()
    
    end_time = datetime.now()
    print('Script finished in', end_time - start_time)
    print('----------------------------------')
