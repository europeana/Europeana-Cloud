FROM openjdk:8
MAINTAINER kadamski

RUN apt-get update \
    && apt-get install -y \
        python=2.7.\* \
        python-pip \
        rsyslog \
        supervisor \
        wget  \
    && rm -rf /var/lib/apt/lists/* \

    # Problem with cqlsh and python https://issues.apache.org/jira/browse/CASSANDRA-11850
    && pip install cqlsh \
    &&  sed -i 's/DEFAULT_PROTOCOL_VERSION = 4/DEFAULT_PROTOCOL_VERSION = 3/' \
        /usr/local/bin/cqlsh
   # If you encounter problem with Err http://deb.debian.org jessie InRelease while building this container it is probably a problem with DNS within our VPN
   # Solution can be found at https://development.robinwinslow.uk/2016/06/23/fix-docker-networking-dns/ -> look for section The permanent system-wide fix
ENV CASSANDRA_DIR /opt/apache-cassandra-2.1.8

RUN wget -q -O - 'http://archive.apache.org/dist/cassandra/2.1.8/apache-cassandra-2.1.8-bin.tar.gz' \
    | tar -C /opt -xz

COPY ./cqls/*.cql /etc/cassandra/
COPY scripts/* /etc/cassandra/
COPY confs/rsyslog.conf /etc/rsyslog.d/50-default.conf
COPY confs/supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY entrypoint.sh /entrypoint.sh

RUN mkdir -p /var/log/supervisor
RUN chmod 555 /entrypoint.sh /etc/cassandra/*

# 7000: intra-node communication
# 7001: TLS intra-node communication
# 7199: JMX
# 9042: CQL
# 9160: thrift service
EXPOSE 7000 7001 7199 8012 9042 9160
#
ENTRYPOINT /entrypoint.sh $CASSANDRA_DIR
