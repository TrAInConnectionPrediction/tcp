FROM python:3.8.1-slim-buster
RUN apt-get update -y && apt-get install -y gcc build-essential

WORKDIR /usr/src/crawler

COPY ./rtd_crawler/crawler_requirements.txt /usr/src/crawler/crawler_requirements.txt
RUN pip install -r crawler_requirements.txt

COPY ./cache/ /usr/src/crawler/cache/
COPY ./helpers/ /usr/src/crawler/helpers/
COPY ./database/ /usr/src/crawler/database/
COPY ./rtd_crawler/ /usr/src/crawler/rtd_crawler/

CMD ["python", "rtd_crawler/recent_change_crawler.py"]