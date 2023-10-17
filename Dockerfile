FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8
LABEL maintainer="Ansuman"

ENV HOST=0.0.0.0 \
    ATTACHMENT_SERVICE=3000 \
    MONGO_PORT=27017
ENV PYTHONUNBUFFERED=1

ARG GITLAB_PIP_TOKEN

WORKDIR /app
COPY requirements.txt ./
RUN pip install -r ./requirements.txt
COPY . ./

EXPOSE $MONGO_PORT
EXPOSE $ATTACHMENT_SERVICE