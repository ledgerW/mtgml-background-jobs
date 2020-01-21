import sys
sys.path.append('../..')
sys.setrecursionlimit(100000)

import os
import boto3
import json
import requests
import math
from time import sleep
import logging
import pandas as pd
logger = logging.getLogger()
logger.setLevel(logging.INFO)

from libs.response_lib import success, failure
from libs.global_cards import scryfall_to_s3


CARDS_URL = os.environ['CARDS_URL']
CARDS_PER_PAGE = int(os.environ['CARDS_PER_PAGE'])
PAGES_PER_WORKER = int(os.environ['PAGES_PER_WORKER'])

client = boto3.client('lambda')


def master(event, context):
    '''
    Trigger: Cron event
    Call Scryfall.com to check number of cards and then distribute
    # collection to worker function below
    '''
    res = requests.get(CARDS_URL.format(1))

    total_cards = res.json()['total_cards']
    total_pages = math.ceil(total_cards / CARDS_PER_PAGE)

    worker_pages = [(first, first+PAGES_PER_WORKER) for first in list(range(1, total_pages+1, PAGES_PER_WORKER))]

    for pages in worker_pages:
        logger.info(pages)
        response = client.invoke(
            FunctionName='mtgml-global-data-prod-prices_worker',
            InvocationType='Event',
            Payload=json.dumps({"first": pages[0], "last": pages[1]}))

    return success({'status': True})


def worker(event, context):
    '''
    Trigger: http post from master
    Accept range of scryfall pages to collect from master,
    call each page,
    then collect them and write to S3
    '''
    bucket = os.environ['PRICES_BUCKET']
    pages = event
    keep_keys = ['id','name','prices']


    # Get cards from Scryfall
    all_prices = pd.DataFrame()
    has_more = True
    for page in range(pages['first'], pages['last']):
        if has_more:
            page_res = requests.get(CARDS_URL.format(page))
            sleep(0.1)
            data = [{key: card[key] for key in keep_keys} for card in page_res.json()['data']]
            for idx, card in enumerate(data):
                data[idx]['usd'] = card['prices']['usd']
                data[idx]['eur'] = card['prices']['eur']
            price_df = pd.read_json(json.dumps(data), orient='records')\
                .drop(columns=['prices'])
            all_prices = pd.concat([all_prices, price_df], axis=0, ignore_index=True)
            has_more = page_res.json()['has_more']
    try:
        status = scryfall_to_s3(bucket, pages['first'], all_prices)
    except:
        e = sys.exc_info()[0]
        logger.info(e)
        return failure({'status': False})

    return success({'status': status})
