FROM openjdk:8
MAINTAINER kadamski

RUN apt-get update \
    && apt-get install -y \
        python2.7 \
        rsyslog \
        supervisor \
        wget  \
    && rm -rf /var/lib/apt/lists/*

RUN wget -q -O - 'http://archive.apache.org/dist/cassandra/2.1.8/apache-cassandra-2.1.8-bin.tar.gz' \
    | tar -C /opt -xz

COPY ./cqls/*.cql /etc/cassandra/
COPY scripts/configure.sh /etc/cassandra/
COPY scripts/initialize_tables.sh /etc/cassandra/
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

ENTRYPOINT /entrypoint.sh