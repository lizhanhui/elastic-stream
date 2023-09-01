# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
FROM ubuntu:20.04

# Do not ask for confirmations when running apt-get, etc.
ENV DEBIAN_FRONTEND noninteractive

# Set the ducker.creator label so that we know that this is a ducker image.  This will make it
# visible to 'ducker purge'.  The ducker.creator label also lets us know what UNIX user built this
# image.
ARG ducker_creator=default
LABEL ducker.creator=$ducker_creator

RUN sed -i "s/archive.ubuntu./mirrors.aliyun./g" /etc/apt/sources.list
RUN sed -i "s/deb.debian.org/mirrors.aliyun.com/g" /etc/apt/sources.list
RUN sed -i "s/security.debian.org/mirrors.aliyun.com\/debian-security/g" /etc/apt/sources.list
RUN sed -i "s/httpredir.debian.org/mirrors.aliyun.com\/debian-security/g" /etc/apt/sources.list

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# JDK
ENV JDK_VERSION 11
ENV JDK_HOME /usr/lib/jvm/java-${JDK_VERSION}-openjdk-amd64
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-${JDK_VERSION}-jdk

ENV PATH $PATH:$JDK_HOME/bin



# RUN pip install -U pip
# RUN pip config set global.index-url http://mirrors.aliyun.com/pypi/simple
# RUN pip config set install.trusted-host mirrors.aliyun.com
# Update Linux and install necessary utilities.
# we have to install git since it is included in openjdk:8 but not openjdk:11
RUN apt update && apt install -y sudo git netcat iptables rsync unzip wget curl jq coreutils openssh-server net-tools vim python3-pip python3-dev libffi-dev libssl-dev cmake pkg-config libfuse-dev iperf traceroute iproute2 iputils-ping && apt-get -y clean
RUN python3 -m pip install -U pip==21.1.1;
RUN pip config set global.index-url http://mirrors.aliyun.com/pypi/simple
RUN pip config set install.trusted-host mirrors.aliyun.com
RUN pip3 install --upgrade cffi virtualenv pyasn1 boto3 pycrypto pywinrm ipaddress enum34 debugpy && pip3 install --upgrade "ducktape>0.8"

# Set up ssh
COPY ./ssh-config /root/.ssh/config
# NOTE: The paramiko library supports the PEM-format private key, but does not support the RFC4716 format.
RUN ssh-keygen -m PEM -q -t rsa -N '' -f /root/.ssh/id_rsa && cp -f /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys
RUN echo 'PermitUserEnvironment yes' >> /etc/ssh/sshd_config

# Install binary test dependencies.
# we use the same versions as in vagrant/base.sh
# ARG ES_MIRROR="https://github.com/AutoMQ/elastic-stream/releases/tag/v0.2.8"
RUN apt-get update
RUN apt-get install -y wget telnet

RUN wget -P /opt "https://github.com/AutoMQ/elastic-stream/releases/download/v0.2.10/pd_0.2.10_amd64.deb"
RUN wget -P /opt "https://github.com/AutoMQ/elastic-stream/releases/download/v0.2.10/range-server_0.2.10_amd64.deb"
ADD tools-1.1.0-SNAPSHOT-jar-with-dependencies.jar /opt
RUN dpkg --recursive --install /opt


ARG UID="1000"


# Set up the ducker user.
RUN useradd -u $UID -ms /bin/bash ducker \
  && mkdir -p /home/ducker/ \
  && rsync -aiq /root/.ssh/ /home/ducker/.ssh \
  && chown -R ducker /home/ducker/ /mnt/ /var/log/ \
  && echo "PATH=$(runuser -l ducker -c 'echo $PATH'):$JAVA_HOME/bin" >> /home/ducker/.ssh/environment \
  && echo 'PATH=$PATH:'"$JAVA_HOME/bin" >> /home/ducker/.profile \
  && echo 'ducker ALL=(ALL) NOPASSWD: ALL' >> /etc/sudoers

USER ducker

CMD sudo service ssh start && tail -f /dev/null
