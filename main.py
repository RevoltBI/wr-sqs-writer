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

columns = {}

with open(parameters['INPUT_FORMAT'], mode='rt', encoding='utf-8') as in_file:
    lazy_lines = (line.replace('\0', '') for line in in_file)
    reader = csv.DictReader(lazy_lines, lineterminator=csvlt, delimiter=csvdel, quotechar=csvquo)

    for row in reader:
        columns[row['standard_attribute_name']] = {
            'type': row['json_value_type'],
            'format': row['format_info']
        }

with open(parameters['INPUT'], mode='rt', encoding='utf-8') as in_file:
    lazy_lines = (line.replace('\0', '') for line in in_file)
    reader = csv.DictReader(lazy_lines, lineterminator=csvlt, delimiter=csvdel, quotechar=csvquo)
    for row in reader:
        data = {}

        for key, value in columns.items():
            try:
                if value['type'] == "string":
                    data[key] = string(row[data[key]])

                if value['type'] == "number":
                    if value['format'] == "integer":
                        data[key] = string(row[data[key]])

                    if value['format'] == "float":
                        data[key] = float(row[data[key]])

                if value['type'] == "object":
                    current_data = json.loads(row[data[key]])
                    data[key] = json.dumps(row[data[key]])
            except:
                print('Missing column')

        message = json.dumps(data)

        print(message)

        break

        #try:
        #    sqs_conn.send_message(queue=queue, message_content=message)
        #except:
        #    print("Failed to push message")

print("Job done!")
