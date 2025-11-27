ARG BASE_IMAGE=wavespeed/pytorch-cuda:2.5.0-py312-cu124-fix2
FROM ${BASE_IMAGE} AS dev-base

ENV THIS_DIR=images/test_worker

SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ENV DEBIAN_FRONTEND=noninteractive
ENV SHELL=/bin/bash

WORKDIR /opt/work_dir

RUN apt-get update --yes && \
    apt-get install google-perftools -y && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN $PY -m pip install --no-build-isolation --no-cache-dir -U pip wheel setuptools

COPY ./$THIS_DIR/requirements.txt /opt/work_dir/requirements.txt
RUN $PY -m pip install --no-build-isolation --no-cache-dir -r /opt/work_dir/requirements.txt

# Install wavespeed SDK from local source
COPY . /opt/wavespeed-python
RUN $PY -m pip install --no-build-isolation --no-cache-dir -e /opt/wavespeed-python

COPY ./$THIS_DIR/handler.py /opt/work_dir/handler.py

ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libtcmalloc.so.4

CMD $PY /opt/work_dir/handler.py
