FROM ubuntu:16.04


ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8
ENV HOME /root
ENV PATH $HOME/miniconda/bin:$PATH
ENV PYTHON_VERSION 2.7
ENV APP_PATH /usr/src/app

RUN apt-get update -yqq \
  && apt-get install -yqq --no-install-recommends \
    software-properties-common

# -- Install System dependencies
RUN DEBIAN_FRONTEND=noninteractive apt-get install -yqq --no-install-recommends \
    build-essential \
    python3-pip \
    git \
    wget \
    mysql-client \
  && apt-get -q clean

# -- Install Application into container
RUN set -ex && mkdir -p ${APP_PATH}

WORKDIR ${APP_PATH}

# -- Adding environment files
COPY requirements-dev.txt requirements-dev.txt
COPY requirements.txt requirements.txt
COPY docker_dev/scripts/setup-server.sh docker_dev/scripts/setup-server.sh

# -- Install python environment
RUN docker_dev/scripts/setup-server.sh

