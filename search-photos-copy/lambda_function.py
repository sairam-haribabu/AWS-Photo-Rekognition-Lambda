import json
import requests
from requests.auth import HTTPBasicAuth
import boto3

def get_elastic(labels):
        ELASTIC_SEARCH_HOST='https://search-photo-hpaaisgyzvwcfpjhfvtx44ggp4.us-east-1.es.amazonaws.com/_search?pretty'
        ELASTIC_SEARCH_HTTP_AUTH='CCBD@2photos'

        header = {'Content-Type': 'application/json'}
        query = {
            'size': 10, 
            'query': {
                'match': {
                    'labels': {
                        'query': labels, 
                        'operator': 'AND'
                    }
                }
            }
        }

        response = requests.request('GET', ELASTIC_SEARCH_HOST, data=json.dumps(query), auth=HTTPBasicAuth('master', ELASTIC_SEARCH_HTTP_AUTH), headers=header)
        # print(json.loads(response.text))
        images = json.loads(response.text)['hits']['hits']
        print("images: ", images)
        results = []
        if 'hits' in json.loads(response.text)['hits']:
            for hit in json.loads(response.text)['hits']['hits']:
                result = 'https://ccbd-photos-bucket.s3.amazonaws.com/' + hit["_source"]["objectKey"]
                results.append(result)

        return results

def lambda_handler(event, context):
    print("hello from lf2")
    query=event["key"]
    client = boto3.client('lex-runtime')
    response = client.post_text(
        botName='photobot',
        botAlias='dev',
        userId='test',
        inputText=query
    )
    labels=[]
    
    if (response['slots']['slotOne']):
        labels.append(response["slots"]["slotOne"])
    if (response['slots']['slotTwo']):
        labels.append(response["slots"]["slotTwo"])
    
    for i in range(len(labels)):
        if labels[i][-1] == "s":
            labels[i] = labels[i][:-1]
    
    es_query = ' '.join(labels)
    elasticqueryresults=get_elastic(es_query)
    return {
        'statusCode': 200,
        'headers': {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        'body': list(set(elasticqueryresults))
    }
