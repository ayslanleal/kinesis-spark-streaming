import json
import random
import datetime
import uuid
import boto3

client = boto3.client("firehose",region_name='sa-east-1')


def put_record(record):
    data = json.dumps(record) + "\n"
    response = client.put_record(
        DeliveryStreamName='PUT-S3-kinesis-data',
        Record={"Data": data}
    )
    print(record)

    return response

def create_order():
    return {
        "id": str(uuid.uuid4()),
        "created_at":(datetime.datetime.now() - datetime.timedelta(hours=random.randrange(0,1))).strftime("%Y-%m-%d %H:%M:%S"),
        "value": 39.9,
        "quantity": random.randrange(1,4),
        "product_name": random.choice(["Camiseta","Caneca"]),
        "customer_id": str(uuid.uuid4()) 
    }

while True:
    record = create_order()
    put_record(record)