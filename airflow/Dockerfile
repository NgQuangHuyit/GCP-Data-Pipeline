FROM apache/airflow:2.9.3-python3.9

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow


COPY ./requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt