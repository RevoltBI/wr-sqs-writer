import csv
import json
import boto.sqs
from keboola import docker

cfg = docker.Config('/data/')
parameters = cfg.get_parameters()

print("Starting process...")

SQS_AWS_REGION = parameters['SQS_AWS_REGION']
SQS_AWS_ACCESS_KEY_ID = parameters['SQS_AWS_ACCESS_KEY_ID']
SQS_AWS_SECRET_ACCESS_KEY = parameters['SQS_AWS_SECRET_ACCESS_KEY']
SQS_AWS_QUEUE_NAME = parameters['SQS_AWS_QUEUE_NAME']

sqs_conn = boto.sqs.connect_to_region(
    region_name = SQS_AWS_REGION,
    aws_access_key_id = SQS_AWS_ACCESS_KEY_ID,
    aws_secret_access_key = SQS_AWS_SECRET_ACCESS_KEY
)

queue = sqs_conn.get_queue(queue_name=SQS_AWS_QUEUE_NAME)

csvlt = '\n'
csvdel = ','
csvquo = '"'

with open('/data/in/tables/source.csv', mode='rt', encoding='utf-8') as in_file:
    lazy_lines = (line.replace('\0', '') for line in in_file)
    reader = csv.DictReader(lazy_lines, lineterminator=csvlt, delimiter=csvdel, quotechar=csvquo)
    for row in reader:
        data = {}

        for key, value in row.items():
            data[key] = value

        message = json.dumps(data)

        try:
            sqs_conn.send_message(queue=queue, message_content=message)
        except:
            print("Failed to push message")

print("Job done!")
