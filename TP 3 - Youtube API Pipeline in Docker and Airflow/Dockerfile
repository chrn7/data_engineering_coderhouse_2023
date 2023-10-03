FROM apache/airflow:2.7.1

COPY requirements.txt /

USER airflow

RUN pip install --upgrade pip
RUN pip install -r/requirements.txt
RUN pip install oauth2client
RUN pip install google-api-python-client

USER root

