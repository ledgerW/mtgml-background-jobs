import sys
sys.path.append('../..')

import os
import boto3
import json
import libs.dynamodb_lib as dynamodb_lib
from libs.response_lib import success, failure
from boto3.dynamodb.conditions import Key


def main(event, context):
    try:
        table = os.environ['GLOBAL_CARDS_TABLE']

        params = {
            'KeyConditionExpression': Key('userId').eq(input_id)
        }
        result = dynamodb_lib.call(table, 'query', params)

        response = success(result['Items'])
    except:
        response = failure({'status': False})

    return response
