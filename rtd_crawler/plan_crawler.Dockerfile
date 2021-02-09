# For more information, please refer to https://aka.ms/vscode-docker-python
FROM python:3.8-slim-buster
RUN apt-get update -y && apt-get install -y gcc build-essential

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

WORKDIR /usr/src/crawler

RUN apt-get update -y && apt-get install -y gcc

# install dependencies
RUN pip install --upgrade pip
COPY ./rtd_crawler/crawler_requirements.txt /usr/src/crawler/crawler_requirements.txt
RUN pip install -r crawler_requirements.txt

COPY ./cache/ /usr/src/crawler/cache/
COPY ./helpers/ /usr/src/crawler/helpers/
COPY ./database/ /usr/src/crawler/database/
COPY ./rtd_crawler/ /usr/src/crawler/rtd_crawler/

# Switching to a non-root user, please refer to https://aka.ms/vscode-docker-python-user-rights
RUN useradd appuser && chown -R appuser /usr/src/crawler
USER appuser

# During debugging, this entry point will be overridden. For more information, please refer to https://aka.ms/vscode-docker-python-debug
CMD ["python", "rtd_crawler/plan_crawler.py"]
