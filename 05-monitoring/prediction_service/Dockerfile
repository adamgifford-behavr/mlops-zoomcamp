# syntax=docker/dockerfile:1

FROM python:3.8-slim-buster

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

RUN pip3 install evidently

COPY app.py .
COPY app_test.py .
COPY lin_reg.bin .

ENV FLASK_APP=app_test.py
CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0", "--port=9696"]