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
            current_data = None
            type = value['type']
            format = value['format']

            try:
                current_data = row[key]
            except:
                continue

            export_value = None

            if current_data:
                if type == "string":
                    export_value = str(current_data)

                if type == "object":
                    try:
                        temp_data = json.loads(current_data)

                        export_value = {
                            'id': int(temp_data['id'])
                        }
                    except:
                        export_value = {}

                if type == "number":
                    if format == "integer":
                        export_value = int(float(current_data))

                    if format == "float":
                        export_value = float(current_data)

                if type == "array":
                    temp_data = json.loads(current_data)
                    temp_array = []

                    for value in temp_data:
                        temp_array.append(value)

                    export_value = temp_array

                data[key] = export_value

            continue

        message = json.dumps(data)

        try:
            sqs_conn.send_message(queue=queue, message_content=message)
        except:
            print("Failed to push message")

print("Job done!")
