import re
import json
import uuid
import urllib.parse
import boto3


s3 = boto3.client('s3')


def save_doc_stats(lines, bucket, key_parts, handwriting_pct, public_uuid):
    num_lines = len(lines)
    num_chars = sum([len(line['Text']) for line in lines])

    stats = {
        'workflow': key_parts['workflow'],
        'remainder': key_parts['remainder'],
        'public_uuid': public_uuid,
        'num_lines': num_lines,
        'num_chars': num_chars,
        'handwriting_pct': handwriting_pct
    }

    out_key = f"ocr/stats/{key_parts['workflow']}/{key_parts['remainder']}__{public_uuid}.json"

    print(stats)

    s3.put_object(
        Body=json.dumps(stats),
        Bucket=bucket,
        Key=out_key,
        StorageClass='GLACIER_IR',
        ContentType='application/json'
    )
    return out_key

def get_stats_key(bucket, hit_key):
    matching_stats_prefix = hit_key.replace('ocr/hits', 'ocr/stats').replace('.json', '')

    params = {
        "Bucket": bucket,
        "Prefix": matching_stats_prefix
    }

    matching_keys = [obj['Key'] for obj in s3.list_objects_v2(**params)['Contents']]

    print(matching_keys)
    return matching_keys[0]


def get_payload(event):
    if 'key' in event:
        # Get object from direct payload
        bucket = event['bucket']
        key = event['key']
    elif 'Records' in event:
        # Get the object from a more standard put event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(
            event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    else:
        # Get the object from an EventBridge event
        bucket = event['detail']['bucket']['name']
        key = event['detail']['object']['key']
    return bucket, key


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    bucket, hit_key = get_payload(event)

    # bucket = "covenants-deed-images"
    # hit_key = "ocr/hits/wi-milwaukee-county/17760704/00673792_NOTINDEX_0002.json"

    # Find the stats key that matches the hits key
    stats_key = get_stats_key(bucket, hit_key)

    # Get the OCR JSON
    try:
        ocr_json_key = hit_key.replace('ocr/hits', 'ocr/json')
        print(ocr_json_key)

        content_object = s3.get_object(Bucket=bucket, Key=ocr_json_key)
        ocr_json_response = json.loads(content_object['Body'].read().decode('utf-8'))

    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure it exists and your bucket is in the same region as this function.'.format(ocr_json_key, bucket))
        raise e

    #Get the text blocks
    blocks=ocr_json_response['Blocks']

    page_info = [block for block in blocks if block['BlockType'] == 'PAGE']
    lines = [block for block in blocks if block['BlockType'] == 'LINE']
    words = [block for block in blocks if block['BlockType'] == 'WORD']

    handwriting_words = [word for word in words if word["TextType"] == 'HANDWRITING']
    if len(words) > 0:
        handwriting_pct = round(len(handwriting_words) / len(words), 2)
    else:
        handwriting_pct = 0

    # Note: this is a different regex than the main ocr lambda
    key_parts = re.search('(?P<status>[a-z]+/[a-z]+)/(?P<workflow>[A-z\-]+)/(?P<remainder>.+)__(?P<public_uuid>[A-z0-9]+)\.(?P<extension>[a-z]+)', stats_key).groupdict()

    # Use existing uuid hex
    public_uuid = key_parts['public_uuid']

    page_stats_file = save_doc_stats(lines, bucket, key_parts, handwriting_pct, public_uuid)

    return {
        "statusCode": 200,
        "body": {
            "message": "hello world",
            "bucket": bucket,
            "hit_key": hit_key,
            "stats": page_stats_file,
            "uuid": public_uuid,
            "handwriting_pct": handwriting_pct
        }
    }
