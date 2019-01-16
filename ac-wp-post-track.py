import os, sys, time
import json, html, base64
from datetime import datetime, date, timedelta
import requests
from Laduc_WordPress import WordPressClientInit as WPInit
from Laduc_SQL import SQLClient as RSQL
import pytz
import multiprocessing


def main():
    """
    The main method to execute all the operations related to WordPress posts  
    Parameters : None
    
    Return values: None
    """
    # Fetching current time
    start_time = datetime.now()
    print(start_time)
    # WP is used for initialization of WordPressClient
    WP = WordPressClient(WPInit())
    # After is generating time difference of 2 minutes in 'US/Eastern' timezone from where we wanted to track post
    after = (datetime.now(pytz.UTC)-timedelta(minutes=2)).astimezone(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S')
    # Retrieving all posts for specified period from WordPress
    posts = WP.posts_list(after)

    if posts:
        try:
            # Creating connection with database using SQLClient 
            SQL = RSQL()
        except Exception as e:
            print('SQL Init Error -', repr(e))

        try:
            # cat_ids is object which is fetching all distinct category id's from 'wp_ac_map' Sql table
            cat_ids = SQL.wp_get_cat_ids_valid()
            rows = []
            # Iterating all retrieved posts
            for post in posts:
                # all 'slug' fields available in posts
                cat_slugs = SQL.wp_get_cat_slugs(post['categories'])

                for cat in post['categories']:
                    # If unit cat object is available in all distinct category id's cat_ids variable
                    if cat in cat_ids:
                        # keys = ['wp_id', 'wp_categories', 'wp_categories_ids', 'wp_title', 'wp_content', 'wp_excerpt', 'wp_link', 'wp_json', 'wp_date', 'ac_sent']
                        obj = {}
                        # Pushing keys and values into obj dictionary 
                        obj['wp_id'] = post['id']
                        obj['wp_categories'] = ','.join(cat_slugs)
                        obj['wp_categories_ids'] = ','.join([str(x) for x in post['categories']])
                        obj['wp_title'] = html.unescape(post['title']['rendered']).replace('\u2013','-')
                        obj['wp_content'] = base64.b64encode(bytes(html.unescape(post['content']['rendered']), 'utf-8'))
                        obj['wp_excerpt'] = base64.b64encode(bytes(html.unescape(post['excerpt']['rendered']), 'utf-8'))
                        obj['wp_link'] = post['link']
                        obj['wp_json'] = json.dumps(post)
                        obj['wp_date'] = post['date'].replace('T', ' ')
                        obj['ac_sent'] = 0
                        # Appending obj dictionary into rows list
                        rows.append(obj)
                        break

        except Exception as e:
            print('Post parse error -', repr(e))

        try:
            #if rows list is not empty
            if rows:
                try:
                    # It is pushing rows data into wp_ac_posts Sql table 
                    resp = SQL.wp_push_ac_posts(rows)
                    print(resp)
                except Exception as e:
                    print('SQL - wp_push posts -', repr(e))

            try:
                SQL.close()
            except:
                pass

        except Exception as e:
            print('SQL push Error -', repr(e))

        for post in posts:
            # It is printing all post
            print(post['date'], post['slug'], post['link'])

    end_time = datetime.now()
    print('Script finished in', end_time - start_time)
    print('--------')


class WordPressClient:
    """
    WordPressClientInit is used for interaction with WordPress
    """
    def __init__(self, WPInit):
        """
        Constructor which is assigning WordPress properties 
        Function arguments:
        WPInit   : Contains Configuration for connecting to WordPress  
        
        Return values: None
        """
        self.user = WPInit.user
        self.password = WPInit.password
        self.api_url = WPInit.api_url
        # self.token = WPInit.token
        self.headers = WPInit.headers

    def posts_list(self, after):
        """
        Fetching lists of posts for specified period 
        Function arguments:
        after   :   Time period to fetch the posts
        
        Return values: retrieved posts
        """
        obj = {}
        # obj is dictionary with per_page is key
        obj['per_page'] = 10
        if after:
            obj['after'] = after
        # maximum number of tries 
        tries = 3
        #url is https://laductrading.com/wp-json/wp/v2/posts
        url = self.api_url + 'posts'
        while True:
            try:
                after_str = ' '+str(after) if after else ''
                print('Attempting to list posts since{after}'.format(after=after_str))
                # fetching data from url like
                # https://laductrading.com/wp-json/wp/v2/posts?per_page=10&after=2018-03-15+04%3A25%3A28
                r = requests.get(url, params=obj, headers=self.headers, timeout=30)
                status = r.status_code
                try:
                    if status in [200]:
                        # saving all relevant response into data variable
                        data = r.json()
                        print('Found', len(data), 'matching posts')
                        # returning all relevant response
                        return r.json()
                    else:
                        # if url response is not ok then reducing tries by 1
                        tries -= 1
                        print('No success', r, r.reason, r.content)
                        time.sleep(1)

                except Exception as e:
                    print('Retrying post list', status, obj, r.content)
                    # if Exception occurs then reducing tries by 1
                    tries -= 1
                    time.sleep(1)

            except Exception as e:
                tries -= 3
                print('WP posts_list - main request -',repr(e))

            if tries <= 0:
                print("Couldn't get posts", status, obj, after)
                break


if __name__ == '__main__':
    #Creating a thread calling main funciton
    p = multiprocessing.Process(target=main)
    #Starting the thread
    p.start()
    p.join(55)
    #Killing the thread if it is still alive after the job
    if p.is_alive():
        print("running... let's kill it...")
        print('------')
        print()

        # Terminate
        p.terminate()
        p.join()    
