FROM python:3.10-alpine
RUN apk update && apk add build-base postgresql postgresql-dev libpq

RUN apk add mariadb-connector-c mariadb-dev

RUN mkdir /app
WORKDIR /app
COPY ./requirements.txt .
RUN pip install -r requirements.txt
ENV PYTHONUNBUFFERED 1
COPY . .
