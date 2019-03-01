FROM quay.io/keboola/docker-custom-python:latest

RUN pip install boto

COPY . /code/
WORKDIR /data/

CMD ["python", "-u", "/code/main.py"]
