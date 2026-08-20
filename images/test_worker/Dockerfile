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
# .git is excluded from the build context, so setuptools_scm needs the version passed in
COPY . /opt/wavespeed-python
ARG WAVESPEED_VERSION=0.0.0
RUN SETUPTOOLS_SCM_PRETEND_VERSION_FOR_WAVESPEED="$WAVESPEED_VERSION" \
    $PY -m pip install --no-build-isolation --no-cache-dir -e /opt/wavespeed-python

COPY ./$THIS_DIR/handler.py /opt/work_dir/handler.py

ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libtcmalloc.so.4

RUN useradd -m -u 1000 nonroot && \
    chown -R nonroot:nonroot /opt/work_dir
USER nonroot

CMD $PY /opt/work_dir/handler.py
