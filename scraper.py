import tweepy
import config
import logging
import os
import time
from datetime import date, timedelta
import json
import requests


class MyStreamListener(tweepy.StreamListener):

    def __init__(self, json_path, api=None):
        self.api = api or API()
        self.json_path = json_path

    def on_status(self, status):
        with open(self.json_path, 'a+', encoding='utf-8') as outfile:
            json.dump(status._json, outfile, ensure_ascii=False, indent=2)

    def on_error(self, error):
        if error.status_code == 420:
            logging.warning("Rate limit reached, sleeping for 15 minutes.")
            time.sleep(15 * 60)
        else:
            logging.error("Error raised.")
            if error.response.text:
                logging.error(f'{error.response.text}')
            else:
                logging.error(f'{error.reason}')


def setup():
    # set date ranges
    yesterday = date.today() - timedelta(days=1)
    yesterday = yesterday.strftime('%Y-%m-%d')
    today = date.today().strftime('%Y-%m-%d')
    # get log file
    script_dir = os.path.dirname(__file__)
    file_path = 'logs/' + yesterday + '.log'
    log_path = os.path.join(script_dir, file_path)
    logging.basicConfig(filename=log_path, filemode='a+', format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
    # get correct json file
    script_dir = os.path.dirname(__file__)
    file_path = 'data/' + yesterday + '.json'
    json_path = os.path.join(script_dir, file_path)
    # set Twitter api access
    try:
        auth = tweepy.OAuthHandler(config.consumer_token, config.consumer_secret)
        auth.set_access_token(config.access_token, config.access_token_secret)
        api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    except tweepy.TweepError as e:
        logging.error("Error opening Twitter api.")
        logging.error(e.reason)
    return api, log_path, json_path


def mailer(log_path, json_path):
    request_url = 'https://api.mailgun.net/v2/{0}/messages'.format(config.url)
    r = requests.post(request_url, auth=('api', config.key),
        data={
            'from': config.sender,
            'to': config.recipient,
            'subject': "Today's data",
            'text': "Today's data files and log"
        }, files=[
            ('attachment', open(log_path)),
            ('attachment', open(json_path))
        ]
    )
    if r.ok:
        logging.info("Mail sent.")
    else:
        logging.error("Error in mail sending.")
        logging.error(r.text)


def start_scrape():
    api, log_path, json_path = setup()
    logging.info("Opened Twitter connection.")
    mysl = MyStreamListener(json_path, api)
    mys = tweepy.Stream(auth=api.auth, listener=mysl)
    mys.sample()
    mailer(log_path, json_path)


start_scrape()
