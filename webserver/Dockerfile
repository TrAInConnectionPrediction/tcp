# pull official base image
FROM python:3.8.1-slim-buster

# set work directory
WORKDIR /usr/src/server

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update -y && apt-get install -y gcc

# install dependencies
RUN pip install --upgrade pip
COPY ./webserver/requirements.txt /usr/src/server/requirements.txt
RUN pip install -r requirements.txt

# copy project
COPY ./cache/ /usr/src/server/cache/
COPY ./webserver/ /usr/src/server/webserver/
COPY ./helpers/ /usr/src/server/helpers/
COPY ./database/ /usr/src/server/database/
COPY ./config.py /usr/src/server/config.py
COPY ./webserver_entrypoint.py /usr/src/server/webserver_entrypoint.py