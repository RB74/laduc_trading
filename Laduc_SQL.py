import os, sys, time
import configparser
import mysql.connector


class SQLClient:
    """
    SQLClient is used for interaction with MySQL database 
    """
    real_path = os.path.dirname(os.path.realpath(__file__))
    config_path = real_path+os.path.sep+'config.ini'
    config = configparser.ConfigParser()
    config.read(config_path)

    def __init__(self):
        """
        Constructor which is assigning SQL properties from config.ini to variables  
        Parameters : None
        
        return values: None
        """
        #local     -- sql_local from config.ini for SQL 
        self.local = False
        #loc     -- loc from config.ini for SQL 
        self.loc = 'local' if self.local else 'remote'
        #connection     -- connection to remote SQL using db_connect()
        self.connection = self._db_connect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def _db_connect(self):
        """
        Function assigning SQL properties from config.ini to variables and connecting to MySQL DB
        Parameters : None
        
        return values: mysql 'connection' object 
        """
        return mysql.connector.connect(
            user=self.config['sql_' + self.loc]['user'],
            password=self.config['sql_' + self.loc]['password'],
            host=self.config['sql_' + self.loc]['host'],
            database=self.config['sql_' + self.loc]['db'],
            pool_name='default',
            pool_size=4
        )

    def close(self):
        """
        Function closing connection with MySQL DB
        """
        # print('RDSClient: Connection closed.')
        try:
            self.connection.close()
            return True
        except Exception:
            return False

    def ping(self):
        """
        Function doing handshaking with MySQL DB using ping 
        """
        self.connection.ping(True)

    def wp_update_categories(self, data):
        """
        Function updating categories in the database
        Function arguments:
        data   :  multiple rows with unit object having 'id' and 'slug ' field 
        
         Return values: Returning True when successful & False when failed to update categories 
        """
        # Setting table variable to 'wp_cat_slugs' table
        table = 'wp_cat_slugs'
        # Setting keys
        keys = ['id', 'slug']
        # Iterating over all data 
        for x, row in enumerate(data):
            # Iterating over each row of keys
            for key in keys:
                if key not in row:
                    row[key] = ''
            # it is putting current row into data variable at xth position  
            data[x] = dict(row)

        inserts, values, duplicates = [],[],[]
        # Iterating over each key of keys list
        for key in keys:
            values.append('%('+key+')s')
            duplicates.append(key+'='+'VALUES('+key+')')
        
        inserts = ', '.join(keys)
        values = ', '.join(values)
        duplicates = ','.join(duplicates)
        # Creating Query for operation
        query = """
                INSERT INTO {table}
                    ({inserts})
                VALUES
                    ({values})
                ON DUPLICATE KEY UPDATE
                    {duplicates}
                """.format(table=table, inserts=inserts, values=values, duplicates=duplicates)

        try:
            cursor=self.connection.cursor(dictionary=True)
            cursor.executemany( query, data )
            cursor.close()
            # it is fetching total number of Affected rows by cursor operation
            response = ' '.join(str(x) for x in ['Affected',cursor.rowcount])
            self.connection.commit()
            return True
        except Exception as e:
            print('Issue inserting', len(data), 'rows... Rolling back and retrying', repr(e))
            # If any exception occurs then it is rolling back 
            self.connection.rollback()
            time.sleep(5)
            return False

    def get_wp_post_max_date(self) -> str:
        date_string = ''

        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute("select max(wp_date) as wp_date from wp_ac_posts;")
            result = cursor.fetchone()

            date_string = result['wp_date']

            cursor.close()
        except Exception as e:
            print('SQL get_wp_post_max_date - error', e)
            self.connection.rollback()

        return date_string

    def wp_push_ac_posts(self, data):
        """
        Updating post values to database  
        Function arguments:
        data   :  rows of posts 
        
        Return values: it is returning response object after updating posts into 'wp_ac_posts' Sql table
        """
        # setting table variable to 'wp_ac_posts'
        table = 'wp_ac_posts'
        # these keys are available in each unit of our data variable
        keys = ['wp_id', 'wp_categories', 'wp_categories_ids', 'wp_title', 'wp_content','wp_excerpt', 'wp_link', 'wp_json', 'wp_date', 'ac_sent', 'errors']
        # Iterating over each row of data 
        for x, row in enumerate(data):
            # Iterating over each row of keys
            for key in keys:
                if key not in row:
                    row[key] = ''
                #it is pushing value of key into row dictionary
                row[key] = row[key].decode('utf8') if type(row[key]) == 'string' else row[key]
            # it is putting current row into data variable at xth position  
            data[x] = dict(row)

        inserts, values, duplicates = [],[],[]
        # Iterating over each row of keys
        for key in keys:
            values.append('%('+key+')s')
            if key not in ['ac_sent','errors']:
                duplicates.append(key+'='+'VALUES('+key+')')
        inserts = ', '.join(keys)
        values = ', '.join(values)
        duplicates = ','.join(duplicates)

        # Creating Query for operation
        query = """
                INSERT INTO {table}
                    ({inserts})
                VALUES
                    ({values})
                ON DUPLICATE KEY UPDATE
                    {duplicates}
                """.format(table=table, inserts=inserts, values=values, duplicates=duplicates)

        try:
            cursor=self.connection.cursor(dictionary=True)
            cursor.executemany( query, data )
            cursor.close()
            # it is fetching total number of Affected rows by cursor operation
            response = ' '.join(str(x) for x in ['Affected',cursor.rowcount])
            self.connection.commit()
            return response
        except Exception as e:
            print('SQL wp_push_ac_posts - Issue inserting', len(data), 'rows... Rolling back and retrying', repr(e))
            # If any exception occurs then it is rolling back 
            self.connection.rollback()
            time.sleep(5)
            return 'Failed to update'

    def wp_get_cat_ids_valid(self):
        """
        Fetching all unique category IDs from database  
        Function arguments: None
        
        Return values: it is returning data object which is fetching all distinct category id's from 'wp_ac_map' Sql table 
        """
        # setting table variable to 'wp_ac_map'
        table = 'wp_ac_map'
        # Creating Query for operation
        query = """
                SELECT distinct(wp_category_id) from {table}
                """.format(table=table)
        try:
            cursor=self.connection.cursor(dictionary=True)
            # fetching all distinct category id's from 'wp_ac_map' Sql table and storing into cursor object
            cursor.execute(query)
            # extracting each unit wp_category_id cursor object and pushing into data variable
            data = [int(x['wp_category_id']) for x in list(cursor)]
        except Exception as e:
            print('SQL wp_get_cat_ids_valid -', repr(e))

        return data

    def wp_get_cat_slugs(self, ids):
        """
        Fetching response from "wp_cat_slugs" table and returning that response 
        
        Function arguments:
        data   :  ids - category ids
        
        return values: it is returning 'data' object which is list of all 'slug' field available in posts
        """
        # setting table variable to 'wp_cat_slugs'
        table = 'wp_cat_slugs'
        # Creating Query for operation
        query = """
                SELECT * from {table} where id in {ids}
                """.format(table=table, ids='('+','.join([str(x) for x in ids])+')')
        try:
            cursor=self.connection.cursor(dictionary=True)
            cursor.execute(query)
            # only fetching 'slug' field
            data = [x['slug'] for x in list(cursor)]
        except Exception as e:
            print('SQL wp_get_cat_slugs -', repr(e))

        return data

    def ac_get_map(self):
        """
        Fetching data from "wp_ac_map" table and returning that response 
        Function arguments: None
       
        Return values: it is returning all the rows wp_ac_map table
        """
        # setting table variable to 'wp_ac_map'
        table = 'wp_ac_map'
        # Creating Query for operation
        query = """
                SELECT * from {table}
                """.format(table=table)
        try:
            cursor=self.connection.cursor(dictionary=True)
            # Fetching all the rows in the table
            cursor.execute(query)
            data = list(cursor)
        except Exception as e:
            print('SQL ac_get_map -', repr(e))

        return data

    def ac_get_posts_valid(self):
        """
        Fetching all the valid posts i.e. ac_sent = 0 & with no errors
        Function arguments: None
       
        Return values: it is returning all the valid rows from wp_ac_posts table
        """
        # setting table variable to 'wp_ac_posts'
        table = 'wp_ac_posts'
        # Creating Query for operation
        query = """
                SELECT * from {table} where ac_sent = 0 AND (errors IS NULL OR errors = '')
                """.format(table=table)
        try:
            cursor=self.connection.cursor(dictionary=True)
            # Fetching all the valid posts in the table
            cursor.execute(query)
            data = list(cursor)
        except Exception as e:
            print('SQL ac_get_posts_valid -', repr(e))

        return data

    def ac_update_post(self, post):
        """
        It is updating post details in "wp_ac_posts" table for ac_sent flag and errors
        
        Function arguments:
        post   :  post which needs to be updated
        
        return values: it is returning number of affected rows
        """
        
        #Listing all the errors in the provided post
        post['errors'] = ', '.join(post['errors']) if post['errors'] else ''
        # setting table variable to 'wp_ac_posts'
        table = 'wp_ac_posts'
        # Creating Query for operation
        query = """
                INSERT INTO {table}
                    (wp_id, ac_sent, errors)
                VALUES
                    (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    wp_id=wp_id,
                    ac_sent=VALUES(ac_sent),
                    errors=VALUES(errors)
                """.format(table=table)
        try:
            cursor=self.connection.cursor(dictionary=True)
            # Executing above query by passing required parameters 
            cursor.execute( query, (post['wp_id'], post['ac_sent'], post['errors']) )
            cursor.close()
            # Fetching the count of affected rows 
            response = ' '.join(str(x) for x in ['Affected',cursor.rowcount])
            self.connection.commit()
            return response
        except Exception as e:
            print('SQL ac_update_post - Issue updating 1 row... Rolling back and retrying', repr(e))
            self.connection.rollback()
            time.sleep(5)
            return 'Failed to update'

###################################################### sheets wordpress ######################################33
    # Sheets_wordpress
    def get_tags(self, tags):
        """
        To fetch the tag from database based on tag name
        Function arguments:
        taga: tag names
        
        Return values: matching tag details
        """
        # Setting table variable to 'wp_cat_slugs' table
        table = 'wp_tags'
        # Creating Query for operation
        query = """
                SELECT * from {table} where name in {tags}
                """.format(table=table, tags='('+','.join(["'"+x+"'" for x in tags])+')')

        try:
            cursor=self.connection.cursor(dictionary=True)
            #Fetching all the tag records matching tag names
            cursor.execute(query)
            data = list(cursor)
            group = {}
            #Creating Tag Name:ID pairs
            for x in data:
                group[x['name'].lower()] = int(x['id'])
        except Exception as e:
            print('SQL get_tags', repr(e))
            try:
                print(tags)
            except:
                pass
            group, data = {}, {}

        return group

    def create_tags(self, data):
        """
        To create a new tag in database
        Function arguments:
        data: details of tags to be created
        
        Return values: True/False
        """
        # Setting table variable to 'wp_cat_slugs' table
        table = 'wp_tags'
        # Setting keys
        keys = ['id', 'name']
        #If any data is not containing value for any of the keys, set it blank
        for x, row in enumerate(data):
            for key in keys:
                if key not in row:
                    row[key] = ''
            data[x] = dict(row)

        inserts, values, duplicates = [],[],[]
        #Setting up the values for inserts, values & duplicates
        for key in keys:
            values.append('%('+key+')s')
            duplicates.append(key+'='+'VALUES('+key+')')
        inserts = ', '.join(keys)
        values = ', '.join(values)
        duplicates = ','.join(duplicates)
        # Creating Query for operation
        query = """
                INSERT INTO {table}
                    ({inserts})
                VALUES
                    ({values})
                ON DUPLICATE KEY UPDATE
                    {duplicates}
                """.format(table=table, inserts=inserts, values=values, duplicates=duplicates)

        try:
            cursor=self.connection.cursor(dictionary=True)
            #Creating tags in database
            cursor.executemany( query, data )
            cursor.close()
            #Fetching the affected rows from the operation
            response = ' '.join(str(x) for x in ['Affected',cursor.rowcount])
            self.connection.commit()
            return True
        except Exception as e:
            print('create_tags - Issue inserting', len(data), 'rows... Rolling back and retrying', repr(e))
            self.connection.rollback()
            time.sleep(5)
            return False

    def get_trade_entries(self, u_ids, finished):
        """
        Fetching records from trade entries 
        Function arguments:
        u_ids  : list of all u_id field from every unit of Google spreadsheet data object
        finished : it is flag (True/False)
         
        Return values: 
        data = it is returning data object after getting rows from 'trading_entries' Sql table
        """
        # Setting Val, q for applying it in Sql query
        val = 'not' if not finished else ''
        q = 'u_id' if finished else '*'
        # Setting table for querying
        table = 'trading_entries'
        # Setting our query for fetching data from 'trading_entries' Sql table
        query = """
                SELECT {q} from {table} where {finished} finished and u_id in {u_ids}
                """.format(table=table, u_ids='('+','.join(["'"+x+"'" for x in u_ids])+')', finished=val, q=q)
        try:
            cursor=self.connection.cursor(dictionary=True)
            # Fetching  data from 'trading_entries' Sql table
            cursor.execute(query)
            # Converting overall fetched data into list 
            data = list(cursor)
        except Exception as e:
            print('SQL get_trade_entries', repr(e))
        # returning overall fetched data 
        return data

    def update_trade_entries(self, data):
        """
        Updating trade entries
        Function arguments:
        data  : list of all u_id field from every unit of Google spreadsheet data object
         
        Return values: 
        data = True/False
        """
        # setting table 'trading_entries' to work
        table = 'trading_entries'
        # setting keys which will be available in data
        keys = ['u_id','type','symbol','position','tactic','thesis','underlying_entry_price',
                'stop_loss','profit_exit','entry_price', 'sold_perc', 'exit_price','date_entered',
                'date_exited','notes','profit_loss_perc','profit_loss_gross','status',
                'days_in_trade','month','week','finished']
        # Iterating over whole data
        for x, row in enumerate(data):
            # Iterating over whole keys
            for key in keys:
                # Checking for key in row or not
                if key not in row:
                    # Checking for key is 'finished' or not
                    if key == 'finished':
                        # Checking for entry_price and exit_price of unit object row
                        if row['entry_price'] and row['exit_price']:
                            # Pushing 1 at key location of unit object row  
                            row[key] = 1
                        else:
                            # Pushing 0 at key location of unit object row  
                            row[key] = 0
                    else:
                        # Pushing '' at key location of unit object row  
                        row[key] = ''
            # Pushing dictionary of row at xth location of data
            data[x] = dict(row)

        inserts, values, duplicates = [],[],[]
        #Setting up values for inserts, values, & duplicates
        for key in keys:
            values.append('%('+key+')s')
            duplicates.append(key+'='+'VALUES('+key+')')
        inserts = ', '.join(keys)
        values = ', '.join(values)
        duplicates = ','.join(duplicates)

        # It is setting query for inserting values into trading_entries
        query = """
                INSERT INTO {table}
                    ({inserts})
                VALUES
                    ({values})
                ON DUPLICATE KEY UPDATE
                    {duplicates}
                """.format(table=table, inserts=inserts, values=values, duplicates=duplicates)

        try:
            cursor=self.connection.cursor(dictionary=True)
            # It is querying with Sql
            cursor.executemany( query, data )
            cursor.close()
            # it is fetching total number of Affected rows by cursor operation
            response = ' '.join(str(x) for x in ['Affected',cursor.rowcount])
            self.connection.commit()
            # Returning true value
            return True
        except Exception as e:
            print('update_trade_entries - Issue inserting', len(data), 'rows... Rolling back and retrying', repr(e))
            self.connection.rollback()
            time.sleep(5)
            # Returning FALSE value if any exception occurs
            return False



