FROM --platform=linux/amd64 python:latest

COPY requirements.txt .
RUN pip install -r requirements.txt