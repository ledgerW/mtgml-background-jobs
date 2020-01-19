import sys
sys.path.append('../..')

import os
import boto3
import json
import libs.dynamodb_lib as dynamodb_lib
from libs.response_lib import success, failure


def main(event, context):
    # UPDATE GLOBAL_DECKS_TABLE FROM MPL Decks and other sources
    return success({'result': 'success'})
