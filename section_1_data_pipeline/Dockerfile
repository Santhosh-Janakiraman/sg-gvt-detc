FROM ubuntu:22.10
ARG HOMEDIR=/pipeline
ENV HOMEDIR=/pipeline
USER root:root
RUN apt update
RUN apt install -y python3 python3-pip python3-virtualenv
ENV PYSPARK_DRIVER_PYTHON python3
ENV PYSPARK_PYTHON python3
WORKDIR ${HOMEDIR}
COPY . ${HOMEDIR}
RUN pip3 install --upgrade pip
RUN pip install -r requirements.txt
