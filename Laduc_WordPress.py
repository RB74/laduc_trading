import os, time
import configparser
import requests

class WordPressClientInit:
    """
    WordPressClientInit is used for interaction with WordPress
    """
    #Specifying the path of config.ini file to read the properties from it
    real_path = os.path.dirname(os.path.realpath(__file__))
    sep = os.path.sep
    config_path = real_path+os.path.sep+'config.ini'
    config = configparser.ConfigParser()
    #Here you are getting configuration data from config.ini file
    config.read(config_path)

    def __init__(self):
        """
        Constructor which is assigning WordPress properties from config.ini to variables  
        Parameters : None
        
        return values: None
        """
        #user     -- username from config.ini for Wordpress 
        self.user = self.config.get('WP', 'user')
        #password -- password from config.ini for Wordpress
        self.password = self.config.get('WP', 'password')
        #apptoken -- token from config.ini for Wordpress
        self.apptoken = self.config.get('WP', 'apptoken')
        #api_url  -- 'https://laductrading.com/wp-json/wp/v2/'
        self.api_url = 'https://laductrading.com/wp-json/wp/v2/'
        #headers  -- It is setting headers 
        self.headers = self._gen_headers()
        # self.token = self._check_token()

    def _gen_headers(self):
        """
        Function to generate headers needed to interact with WordPress 
        Parameters : None
        
        Return values: it is returning required headers
        """
        headers = {}
        #Setting Authorization header to provided Token key
        headers['Authorization'] = 'Basic {token}'.format(token=self.apptoken)
        #Adding User Agent to headers, WordPress is rejecting request (403 forbidden) without User Agent header
        headers['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.162 Safari/537.36'
        return headers

    def _check_token(self):
        """
        Function to fetch WordPress token using username & password
        Parameters : None
        
        Return values: Returning Token if authentication successful
        """
        try:
            #Specifying username and password to pass as parameters
            params = {'username':self.user, 'password':self.password}
            #WordPress URL for authentication
            url = 'https://laductrading.com/wp-json/simple-jwt-authentication/v1/token'
            r = requests.post(url, data=params)
            if r and r.status_code in [200]:
                try:
                    #Fetching Token if authentication is successful
                    token = r.json()['token']
                    return token
                except Exception as e:
                    print('WP - _check_token - config set -',repr(e))
            else:
                print('WP - _check_token - request error -', repr(e), r, r.reason, r.content)

        except Exception as e:
            print('WP - _check_token - request -',repr(e))

        return None

    def get_categories(self):
        """
        Fetching all the categories from the WordPress  
        Parameters : None
        
        Return values: it is returning list of all categories available
        """
        obj = {}
        #Specifying how many categories per page
        obj['per_page'] = 100
        #It will try for 3 times 
        tries, page = 3, 1
        master = []
        while True:
            try:
                obj['page'] = page
                #Building the URL to fetch categories
                url = self.api_url + 'categories'
                # r variable is fetching web page from a url passing the parameters
                r = requests.get(url, headers=self.headers, params=obj)
                # status variable is having status code of called api or url
                status = r.status_code
                # if status is OK then api sends [200] status response  
                if status in [200]:
                    if r.json():
                        page += 1
                        # it is storing all the categories into master list 
                        master.extend(r.json())
                    else:
                        # When no response data received then empty list will be returned
                        return master
                else:
                    #If there the response code in NOT OK, then again try
                    tries -= 1
                    #But sleep for 2 seconds before trying
                    time.sleep(2)
            except Exception as e:
                #if there is an exception, remove all the tries
                tries -= 3
                print('WP get_categories -', repr(e))

            #Stop when number of tries are done
            if tries <= 0:
                break
