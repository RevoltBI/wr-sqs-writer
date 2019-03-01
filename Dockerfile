FROM quay.io/keboola/docker-custom-python:latest

COPY . /code/
WORKDIR /data/

RUN pip install boto
CMD ["python", "-u", "/code/main.py"]
