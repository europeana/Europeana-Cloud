FROM ubuntu:16.04
MAINTAINER kadamski

RUN apt-get update && apt-get install -y \
    keystone \
    memcached \
    pwgen \
    python-keystoneclient \
    python-memcache \
    python-netifaces \
    python-swiftclient \
    python-swiftclient \
    python-xattr \
    rsync \
    supervisor \
    swift-account \
    swift-container \
    swift-object \
    swift-object \
    swift-plugin-s3 \
    swift-proxy \
    swift
#Only for development
RUN apt-get update && apt-get install -y \
    vim \
    net-tools \
    curl


ADD files/keystoneconfigure.sh /keystoneconfigure.sh
RUN chmod 555 /keystoneconfigure.sh
RUN set -x && /keystoneconfigure.sh

RUN mkdir -p /var/log/supervisor
ADD files/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

#
# Swift configuration
# - Partially fom http://docs.openstack.org/developer/swift/development_saio.html
#

# not sure how valuable dispersion will be...
ADD files/dispersion.conf /etc/swift/dispersion.conf
ADD files/rsyncd.conf /etc/rsyncd.conf
ADD files/swift.conf /etc/swift/swift.conf
ADD files/proxy-server.conf /etc/swift/proxy-server.conf
ADD files/account-server.conf /etc/swift/account-server.conf
ADD files/object-server.conf /etc/swift/object-server.conf
ADD files/container-server.conf /etc/swift/container-server.conf
ADD files/proxy-server.conf /etc/swift/proxy-server.conf
ADD files/startmain.sh /usr/local/bin/startmain.sh
RUN chmod 755 /usr/local/bin/*.sh

EXPOSE 8888

CMD /usr/local/bin/startmain.sh
