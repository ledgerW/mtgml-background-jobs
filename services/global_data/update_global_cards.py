import sys
sys.path.append('../..')
sys.setrecursionlimit(100000)

import os
import boto3
import json
import requests
import math
from time import sleep
import pandas as pd
import numpy as np
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

import libs.dynamodb_lib as dynamodb_lib
from libs.response_lib import success, failure


CARDS_URL = os.environ['CARDS_URL']
CARDS_PER_PAGE = os.environ['CARDS_PER_PAGE']
PAGES_PER_WORKER = os.environ['PAGES_PER_WORKER']
BATCH_LIMIT = os.environ['BATCH_LIMIT']

client = boto3.client('lambda')


def save_scryfall_page(table, page):
    dynamo_batches = [(first, first+BATCH_LIMIT) for first in list(range(0, CARDS_PER_PAGE, BATCH_LIMIT))]

    res = requests.get(CARDS_URL.format(page))
    for batch in dynamo_batches:
        sleep(0.3)
        cards = res.json()['data'][batch[0]:batch[1]]
        for idx, card in enumerate(cards):
            for key, val in card.items():
                cards[idx][key] = dynamodb_lib.batch_format(val)
            cards[idx]['cardId'] = cards[idx]['id']

        # Save to Dynamo
        RequestItems = {
            table: [
                {
                    'PutRequest': {
                        'Item': card
                    }
                } for card in cards
            ]
        }
        try:
            dynamo_res = dynamodb_lib.call(table, 'batch_write_item', RequestItems)
            if len(dynamo_res['UnprocessedItems'])>0:
                logger.info('Number of UnprocessedItems: {}'.format(len(dynamo_res['UnprocessedItems'])))
        except e:
            logger.info(page)
            logger.info(e)
    return res.json()['has_more']


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
            FunctionName='mtgml-global-data-prod-cards_worker',
            InvocationType='Event',
            Payload=json.dumps({"first": pages[0], "last": pages[1]}))

    return success({'status': True})


def worker(event, context):
    '''
    Trigger: http post from master
    Accept range of scryfall pages to collect from master,
    call each page,
    then collect them and load to GLOBAL_CARDS_TABLE
    '''
    table = os.environ['GLOBAL_CARDS_TABLE']
    pages = event

    # Get cards from Scryfall
    has_more = True
    for page in range(pages['first'], pages['last']):
        if has_more:
            sleep(0.1)
            try:
                has_more = save_scryfall_page(table, page)
            except:
                logger.info(page)
                return failure({'status': False})
    return success({'status': True})
