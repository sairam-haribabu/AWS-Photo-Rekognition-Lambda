import json
import urllib.parse
import boto3
import datetime
import requests
from requests.auth import HTTPBasicAuth

print('Loading function index-live demo')


def detect_labels(photo, bucket):
    client=boto3.client('rekognition')

    response = client.detect_labels(Image={'S3Object':{'Bucket':bucket,'Name':photo}},
        MaxLabels=10)

    print('Detected labels for ' + photo) 
    print()   
    for label in response['Labels']:
        print ("Label: " + label['Name'])
        print ("Confidence: " + str(label['Confidence']))
        # print ("Instances:")
        # for instance in label['Instances']:
        #     print ("  Bounding box")
        #     print ("    Top: " + str(instance['BoundingBox']['Top']))
        #     print ("    Left: " + str(instance['BoundingBox']['Left']))
        #     print ("    Width: " +  str(instance['BoundingBox']['Width']))
        #     print ("    Height: " +  str(instance['BoundingBox']['Height']))
        #     print ("  Confidence: " + str(instance['Confidence']))
        #     print()

        # print ("Parents:")
        # for parent in label['Parents']:
        #     print ("   " + parent['Name'])
    return [l['Name'] for l in response['Labels']]
    

def put_elastic(obj):
        ELASTIC_SEARCH_HOST='https://search-photo-hpaaisgyzvwcfpjhfvtx44ggp4.us-east-1.es.amazonaws.com/photos/_doc'
        ELASTIC_SEARCH_HTTP_AUTH='CCBD@2photos'

        header = {'Content-Type': 'application/json'}

        response = requests.post(ELASTIC_SEARCH_HOST, data=json.dumps(obj), auth=HTTPBasicAuth('master', ELASTIC_SEARCH_HTTP_AUTH), headers=header)
        print("Elastic response: ")
        print(json.loads(response.text))
        return json.loads(response.text)


def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    print(event)
    # Get the object from the event and show its content type
    s3 = boto3.client('s3')
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print("Here index")
    print(key)
    try:
        custom_labels=[]
        response = s3.get_object(Bucket=bucket, Key=key)
        print(response)
        print("CONTENT TYPE: " + response['ContentType'])
        if(response['Metadata']):
            print("customlabelsfromuser" + response['Metadata']['customlabel'])
            custom_labels = list(response['Metadata']['customlabel'].split(","))

        # detect labels in the image using Rekognition
        labels=detect_labels(key, bucket)
        labels=custom_labels+labels
        labels = list(set(labels))
        print(labels)
        print("Labels detected!!!: " + str(len(labels)))
        

        obj = {
           "objectKey": key,
           "bucket": bucket,
           "createdTimestamp": response["LastModified"].strftime("%Y-%m-%dT%H:%M:%S"),
           "labels": labels
        }
        
        print(obj)
        return put_elastic(obj)
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
