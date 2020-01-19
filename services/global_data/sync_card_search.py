import sys
sys.path.append('../..')

import os
import boto3
import json
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import libs.dynamodb_lib as dynamodb_lib
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def main(event, context):
    if True:
        logger.info('Number of Records: {}'.format(len(event['Records'])))
        return success({'status': True})
    else:
        host = os.environ['CARD_SEARCH_DOMAIN_ENDPOINT']
        region = 'us-east-1'
        service = 'es'
        cred = boto3.Session().get_credentials()
        awsauth = AWS4Auth(cred.access_key, cred.secret_key, region, service)

        es = Elasticsearch(hosts = [{'host': host, 'port': 443}],
                           http_auth = awsauth,
                           use_ssl = True,
                           verify_certs = True,
                           connection_class = RequestsHttpConnection)

        # COMPLETE ELASTIC SEARCH SYNC PROCESS!!!!
        for record in event['Records']:
            # Get the primary key for use as the Elasticsearch ID
            id = record['dynamodb']['Keys']['id']['S']

            if record['eventName'] == 'REMOVE':
                r = requests.delete(url + id, auth=awsauth)
            else:
                document = record['dynamodb']['NewImage']
                r = requests.put(url + id, auth=awsauth, json=document, headers=headers)
