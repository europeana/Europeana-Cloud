FROM ubuntu:16.04
MAINTAINER kadamski
# Based on https://github.com/chalianwar/docker-swift-object

RUN apt-get update && apt-get install -y \
    keystone \
    keystone \
    memcached \
    pwgen \
    python-keystone \
    python-keystoneclient \
    python-memcache \
    python-netifaces \
    python-swiftclient \
    python-xattr \
    rsync \
    rsyslog \
    supervisor \
    swift \
    swift-account \
    swift-container \
    swift-object \
    swift-plugin-s3 \
    swift-proxy \
    && rm -rf /var/lib/apt/lists/*

#Configuraton and executable files preparation
RUN rm /etc/rsyslog.d/*
COPY confs/rsyslog.conf /etc/rsyslog.d/50-default.conf
COPY confs/swift/* /etc/swift/
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
COPY scripts/keystoneconfigure.sh /keystoneconfigure.sh
COPY confs/supervisord.conf /etc/supervisor/conf.d/supervisord.conf
RUN mkdir -p /var/log/supervisor && chmod 555 /keystoneconfigure.sh /usr/local/bin/*.sh


EXPOSE 8888 5000

ENTRYPOINT entrypoint.sh
