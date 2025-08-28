FROM python:3.11.7

COPY requirements*.txt /tmp/
RUN --mount=type=cache,target=/root/.cache \
    pip install -r /tmp/requirements.txt

RUN useradd --create-home --shell /bin/bash newssearch
USER newssearch

COPY newssearch /home/newssearch
WORKDIR /home/newssearch

ENV PYTHONPATH=/home
