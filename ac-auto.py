import os
import time
import urllib
import base64
import requests
import configparser
import multiprocessing
from datetime import datetime
from bs4 import BeautifulSoup as BS
from Laduc_SQL import SQLClient as RSQL


# Categories to be formatted as trade alerts in AC.
TRADE_ALERT_CATEGORIES = [
    'lotto',
    'advanced-options',
    'chase',
    'alert-test',
    'jot-test',
    'quant',
    'swing',
    'trade-alerts',
    'trend',
    'idea'
]


def _format_percent(n):
    """
    Ensures % format (I48).
    in/out:
        7500: 75%
        75.5: 75.5%
        2500: 25%
        25%: 25%
        25.00%: 25.00%
    :param n: (str, int, float)
        A value that should look like N.NN%
    :return: (?)
        Tries returning a string but
        if there's an issue parsing the number
        the original value is returned.
    """
    try:
        n = float(n)
        if n > 100:
            n = n/100
        if int(n) == n:
            n = int(n)
        else:
            n = round(n, 2)
        return '{}%'.format(n)
    except:
        return n


def _format_decimal(n, divide_ge_100=False):
    try:
        n = float(n)
        if n > 100 and divide_ge_100:
            n = n/100
        if int(n) == n:
            n = int(n)
        else:
            n = round(n, 2)
        return str(n)
    except:
        return n


def main():
    """
    The main method to execute all the operations related to ActiveCampaign  
    Parameters : None
    
    Return values: None
    """
    # Calling the function to read the properties from config.ini
    set_globals()
    # Setting the current time
    start_time = datetime.now()
    print(start_time)

    #Setting the TEST_MODE as global variable to specify if this is Test (True) or Production (False)
    global TEST_MODE
    TEST_MODE = False

    c_map, posts_valid = {}, []
    try:
        # Database connection
        SQL = RSQL()

        # Fetch rows from "wp_ac_map" table
        resp = SQL.ac_get_map()

        # Map wp category to mapping record.
        for obj in resp:
            c_map[obj['wp_category']] = dict(obj)

        # Retrieve valid posts to be sent to AC
        posts_valid = SQL.ac_get_posts_valid()

        SQL.close()
    except Exception as e:
        print('Init Error -', repr(e))

    # Send AC emails according to mapping rules.
    if c_map and posts_valid:
        templates = {}
        AC = ActiveCampaignClient(c_map)

        for post in posts_valid:

            # Split post categories
            cats = post['wp_categories'].split(',')

            # Use last or first wp category
            if len(cats) > 1:
                cat = cats[-1]
            else:
                cat = cats[0]

            # Skip posts w/o WP categories.
            if not cat:
                continue

            # Get category mapping.
            # Continuing on error here 9/24/2018
            try:
                obj = c_map[cat]
            except KeyError:
                print("KeyError: missing mapping record for wordpress category '{}' "
                      "- please add mapping record to database: DigitalOcean.MySQL.wp_ac_map".format(cat))
                continue

            # Retrieving template id from cmap corresponding to associated category 
            template_id = obj['ac_template_id']
            if template_id in templates:
                # Retrieving template content based upon template id 
                template_content = templates[template_id]
            else:
                # Fetching template content from WordPress for given template id
                template_content = AC.message_template_list(template_id)
                templates[template_id] = template_content

            # Adding message using ActiveCampaignClient providing template and post details
            msg_id, errors = AC.message_add(template_content, post, obj)

            # If message added, then create campaign
            if msg_id:
                # creating campaign using active client for the given campaign including message created earlier
                resp, errors = AC.campaign_create(post['wp_title'], obj, msg_id, errors)
                if resp['result_message'] == 'Campaign saved':
                    post['ac_sent'] = 1
            else:
                # if message not added, then it adds error message to the errors
                resp = 'No msg_id'
                errors.append(resp)

            post['errors'] = errors

            try:
                print('Updating', post['wp_id'], resp)
                # Creating database connection using SQLClient
                SQL = RSQL()
                # it is updating post details in database
                resp = SQL.ac_update_post(post)
                # Closing the SQL connection
                SQL.close()
                print(resp)
            except Exception as e:
                print('Main - Update post after success - ', repr(e))
    else:
        # If there is no valid posts found
        print(len(posts_valid), 'valid posts')

    end_time = datetime.now()
    print('Script finished in', end_time - start_time)
    print('--------')


class ActiveCampaignClient:
    """
    ActiveCampaignClient is used for interaction with ActiveCampaign 
    """
    def __init__(self, c_map):
        """
        Constructor which is setting parameters and assigning values to variables 
        Function arguments:
        c_map   :  rows of ac_map rows 
        
        Return values: None
        """
        #Setting up the parameters
        self.params = self._params_set()
        #setting base_url https://wdp60034.api-us1.com/admin/api.php
        self.base_url = 'https://wdp60034.api-us1.com/admin/api.php'
        self.c_map = dict(c_map)

    def _params_set(self):
        """
        Setting parameters and assigning values to variables 
        Function arguments: None
        
        Return values: params variable which gets configurations from config file 
        """
        params = {}
        params['api_key'] = config['AC']['api_key']
        params['api_output'] = 'json'
        return params

    def campaign_send(self, campaign, msg_id):
        """
        Resending campaign with some revision
        Function arguments: None
        
        Return values: None 
        """
        print('Trying to send', campaign)
        #Setting parameters to pass to resend the campaign
        params = dict(self.params)
        params['api_action'] = 'campaign_send'
        params['campaignid'] = self.c_map[campaign]
        params['action'] = 'send'
        params['type'] = 'mime'
        params['messageid'] = msg_id

        try:
            #Calling the ActiveCampaign URL with parameters
            r = requests.get(self.base_url, params=params)
            #Fetching response
            resp = r.json()
            print(resp)
        except Exception as e:
            print('AC Client - campaign_send -', repr(e))

    def campaign_list(self, ids):
        """
        Fetching list of all the campaigns for specified campaign ids
        Function arguments: ids - campaign ids
        
        Return values: return msg_id associated with response 
        """
        try:
            #Setting up required parameters 
            params = dict(self.params)
            params['api_action'] = 'campaign_list'
            params['ids'] = ','.join(ids)
            params['full'] = 1
        except Exception as e:
            print('AC Client - campaign_list - params -',repr(e))
        try:
            r = requests.get(self.base_url, params=params)
            resp = r.json()
            try:
                msg_id = resp['0']['messages'][0]['id']
                return msg_id
            except Exception as e:
                print('AC Client - campaign_list - response -', repr(e))
        except Exception as e:
            print('AC Client - campaign_list - request -', repr(e))

    def campaign_create(self, name, obj, msg_id, errors):
        """
        Creating campaign using the details provided
        Function arguments: 
        name - Name of the campaign
        obj - campaign details
        msg_id - message id associated with campaign
        errors - any errors
        
        Return values: returns received response and errors 
        """
        try:
            # Setting up required parameters
            params = dict(self.params)
            params['api_action'] = 'campaign_create'
            params['type'] = 'single'
            params['sdate'] = str(datetime.now())
            params['status'] = 1
            params['public'] = 0
            params['tracklinks'] = 'all'
            #If TEST_MODE is True, set the item differently 
            if TEST_MODE:
                params['p[{item}]'.format(item='17')] = '17'
            else:
                #If Production, fetch the items from ac_list_ids
                for item in obj['ac_list_ids'].split(','):
                    params['p[{item}]'.format(item=item)] = item
            params['m[{m_id}]'.format(m_id=msg_id)] = 100
            params['name'] = name
        except Exception as e:
            msg = 'AC Client Exception - campaign_create - params - {error}'.format(error=repr(e))
            errors.append(msg)
            print(msg)


        resp = {'result_message':'init'}
        tries = 3.0
        while tries > 0:
            try:
                #Making request to ActiveCampaign for creating campaign passing parameters
                r = requests.post(self.base_url, data=params)
                if r.status_code in [200, 201]:
                    #If response is OK, the get the response data
                    resp = r.json()
                    break
                else:
                    #Else keep trying
                    tries -= 1
                    time.sleep(1)
                    #Adding error message to Errors
                    errors.append('AC Client Error - campaign_create - request - {status} - {reason}'.format(status=r.status_code, reason=r.reason))
            except Exception as e:
                tries -= 1
                msg = 'AC Client Exception - campaign_create - request - {error}'.format(error=repr(e))
                errors.append(msg)
                print(msg)

            if tries <= 0 :
                msg = 'Out of tries'
                errors.append( 'AC Client Error - campaign_create - request - {error}'.format( error=str(msg) ) )
                resp['result_message'] = str(msg)
                break

        return resp, errors

    def campaign_get(self, campaign):
        pass

    def message_template_list(self, template_id):
        """
        Fetching list of message templates from ActiveCampaign
        Function arguments: 
        template_id - template id 
        
        Return values: returns received template 
        """
        try:
            #Setting up the required parameters
            params = dict(self.params)
            params['api_action'] = 'message_template_list'
            params['ids'] = template_id
        except Exception as e:
            print('AC Client - message_template_list - params -',repr(e))

        try:
            #Making request to ActiveCampaign with required parameters
            r = requests.post(self.base_url, data=params)
            #Fetching response
            resp = r.json()
            try:
                return resp['0']['content'].replace('\xa0',' ').strip()
            except Exception as e:
                print('AC Client - message_template_list - resp -', repr(e))
                print('Response', resp)
        except Exception as e:
            print('AC Client - message_template_list - request -', repr(e))

    # gets a message based on the ID
    # gets a message based on the ID
    def message_get(self, msg_id):
        pass

    def message_add(self, template_content, post, obj):
        """
        Creates a new message from the standard templated message
        Function arguments: 
        template_content - content of the template to be used for the message
        post - post details
        obj - ac_map details
        
        Return values: returns received response id or errors 
        """
        errors = []

        # Build message parameters from wordpress post.
        try:
            """
            <div id="email-content">
                <a href="{{{post_url}}}">
                    <h2>{{{post_title}}}</h2>
                </a>
                <h4>{{{post_time}}}</h4>
                <div id="post-content">
                    {{{post_content}}}
                </div>
            </div>
            """
            # Unquote all string values in post dict.
            for k, v in post.items():
                post[k] = urllib.parse.unquote(v) if type(v) == 'string' else v

            # Decode post content
            wp_content = post['wp_excerpt'] if obj['excerpt'] else post['wp_content']
            post_content = base64.b64decode(wp_content).decode('utf8')

            # Drop WooCommerce div from post content
            post_content, errors = handle_special(
                post, post_content, obj['wp_category']
            )
            if not post_content:
                errors.append('post_content')
                print('Error with post content')
                return None, errors

            # Drop seconds off post timestamp.
            post['wp_date'] = datetime\
                .strptime(post['wp_date'], '%Y-%m-%d %H:%M:%S')\
                .strftime('%Y-%m-%d %H:%M')

            # Set template content variables
            template_content = urllib.parse.unquote(template_content)
            post_html = template_content\
                .replace('{{{post_url}}}',     post['wp_link'])\
                .replace('{{{post_title}}}',   post['wp_title'])\
                .replace('{{{post_time}}}',    post['wp_date'])\
                .replace('{{{post_content}}}', post_content)

            # Compose post parameters
            params = dict(self.params)
            params['api_action'] = 'message_add'
            params['format'] = 'mime'
            params['subject'] = post['wp_title']
            params['fromemail'] = 'info@laductrading.com'
            params['fromname'] = 'LaDucTrading'
            params['replyto'] = 'support@laductrading.com'
            params['htmlconstructor'] = 'editor'
            params['html'] = post_html
            params['priority'] = '3'
            params['charset'] = 'utf-8'
            if TEST_MODE:
                params['p[{item}]'.format(item='17')] = '17'  # test-list only.
            else:
                # If Production, use items from ac_list_ids
                for item in obj['ac_list_ids'].split(','):
                    params['p[{item}]'.format(item=item)] = item
        except Exception as e:
            msg_err = 'AC Client - message_add - params - {error}'.format(error=repr(e))
            print(msg_err)
            errors.append(msg_err)

        else:
            # Post to ActiveCampaign
            try:
                r = requests.post(self.base_url, data=params)
                resp = r.json()
                try:
                    return resp['id'], errors
                except Exception as e:
                    msg_err = 'AC Client - message_add - resp - {error}'.format(error=repr(e))
                    print(msg_err)
                    errors.append(msg_err)

            except Exception as e:
                msg_err = 'AC Client - message_add - request - {error}'.format(error=repr(e))
                print(msg_err)
                errors.append(msg_err)

        return None, errors


def handle_special(post, post_content, cat):
    """
    Cleans up the post details & contents before creating the campaign
    Function arguments: 
    post - post details
    post_content - HTML content of the post
    cat - category
    
    Return values: returns formatted content or errors 
    """
    errors = []
    new_content = []
    try:
        # Parse the post HTML
        soup = BS(post_content, 'html5lib')
    except Exception as e:
        print('Handle_special - soup -', repr(e))
        errors.append(repr(e))
        soup = ''

    if not soup:
        return '', errors

    try:
        # Drop 'woocommerce' divs
        soup.find("div", 'woocommerce').extract()
    except:
        pass

    # Handles jot-test and trade alert formatting by transposing columns.
    # Updated 2018-10-07 to support multiple trade types.
    if cat in TRADE_ALERT_CATEGORIES:
        try:
            ta = soup.find('table', id='trade-alert')
            if not ta:
                return post_content, errors

            headers = [x.text for x in ta.find_all('th')]
            data = [x.text for x in ta.find_all('td')]

            caption = '<h2 style="color:#222;">{caption}</h2>'.format(caption=soup.caption.text)
            for x, header in enumerate(headers):
                value = data[x]

                # Maybe filter value
                if '%' in header:                           # Ensure % format (I48)
                    value = _format_percent(value)
                elif 'DAYS IN TRADE' == header.upper():     # (I66)
                    value = _format_decimal(value, divide_ge_100=True)

                new_content.append(
                    '<strong style="font-size:14px;font-family:Arial;">'
                    '{header}: '
                    '</strong>'
                    '<span style="font-size:16px;color:#4db0cc;font-weight:bold;font-family:Arial;">'
                    '{value}'
                    '</span>'.format(header=header, value=value))

            new_content = '<br>'.join(new_content)
        except Exception as e:
            print('Handle_special - trade-alerts -', repr(e))
            errors.append(repr(e))

    # Handles free-video 
    elif cat in ['free-video']:
        try:
            link = post['wp_link']
            parts = [x for x in post['wp_link'].split('/') if x]
            if 'free-video' not in parts[-1]:
                try:
                    x = soup.find('span', id='premium-row').extract()
                except:
                    pass
                return str(post_content), errors
            else:
                return str(post_content).replace(
                    '{{{premium-video-url}}}',
                    link.replace('/free-video', '/premium-video')
                ), errors

        except Exception as e:
            print('Handle_special - free-video -',repr(e))
            errors.append(repr(e))
    else:
        new_content = str(post_content)

    return new_content, errors


def set_globals():
    """
    Retrieving configuration properties for config.ini file and setting global variable config
    Function arguments: None
    
    Return values: None 
    """
    global config
    real_path = os.path.dirname(os.path.realpath(__file__))
    config_path = real_path+os.path.sep+'config.ini'
    config = configparser.ConfigParser()
    config.read(config_path)


if __name__ == '__main__':
    #Creating a thread calling main funciton
    p = multiprocessing.Process(target=main)
    #Starting the thread
    p.start()
    p.join(30)
    #Killing the thread if it is still alive after the job
    if p.is_alive():
        print("running... let's kill it...")
        print('------')
        print()

        # Terminate
        p.terminate()
        p.join()
    
    
