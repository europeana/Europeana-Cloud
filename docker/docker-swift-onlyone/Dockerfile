FROM ubuntu:16.04
MAINTAINER kadamski

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
    swift \
    swift-account \
    swift-container \
    swift-object \
    swift-plugin-s3 \
    swift-proxy

#Only for development
RUN apt-get install -y \
    vim \
    net-tools \
    curl \
    ngrep

RUN rm /etc/rsyslog.d/*
COPY files/rsyslog.conf /etc/rsyslog.d/50-default.conf
COPY files/config/* /etc/swift/
COPY files/startmain.sh /usr/local/bin/startmain.sh
COPY files/keystoneconfigure.sh /keystoneconfigure.sh
COPY files/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

RUN chmod 555 /keystoneconfigure.sh /usr/local/bin/*.sh
RUN set -x && /keystoneconfigure.sh
RUN mkdir -p /var/log/supervisor

EXPOSE 8888 5000

CMD startmain.sh
