FROM python:3.11-slim

ADD https://github.com/aptible/supercronic/releases/download/v0.2.29/supercronic-linux-amd64 /usr/local/bin/supercronic
RUN chmod +x /usr/local/bin/supercronic

WORKDIR /

COPY scraper_service /scraper_service
COPY data_processing_service /data_processing_service
COPY data_ingestion_service /data_ingestion_service
COPY scheduler_service/scheduler.cron /scheduler.cron

COPY scraper_service/requirements.txt /scraper_service/requirements.txt
COPY data_processing_service/requirements.txt /data_processing_service/requirements.txt
COPY data_ingestion_service/requirements.txt /data_ingestion_service/requirements.txt

RUN pip install --no-cache-dir -r /scraper_service/requirements.txt \
    && pip install --no-cache-dir -r /data_processing_service/requirements.txt \
    && pip install --no-cache-dir -r /data_ingestion_service/requirements.txt

CMD ["supercronic", "/scheduler.cron"]
