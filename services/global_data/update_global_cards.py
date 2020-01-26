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
logger = logging.getLogger()
logger.setLevel(logging.INFO)

from libs.response_lib import success, failure
from libs.global_cards_lib import scryfall_to_dynamo, scryfall_to_elastic


CARDS_URL = os.environ['CARDS_URL']
CARDS_PER_PAGE = int(os.environ['CARDS_PER_PAGE'])
PAGES_PER_WORKER = int(os.environ['PAGES_PER_WORKER'])
BATCH_LIMIT = int(os.environ['DYNAMO_BATCH_LIMIT'])

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
    domain = os.environ['CARD_SEARCH_DOMAIN_ENDPOINT']
    pages = event

    # Get cards from Scryfall
    has_more = True
    for page in range(pages['first'], pages['last']):
        page_res = requests.get(CARDS_URL.format(page))
        if has_more:
            sleep(0.1)

            try:
                has_more = scryfall_to_dynamo(table, page_res)
            except:
                logger.info(page)
                return failure({'status': False})

    return success({'status': True})
