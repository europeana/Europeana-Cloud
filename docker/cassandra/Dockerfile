FROM ubuntu:14.04
MAINTAINER kadamski

RUN apt-get update \
    && apt-get install -y \
        openjdk-7-jdk \
        python \
        wget  \
    && rm -rf /var/lib/apt/lists/*

RUN wget -q -O - 'http://archive.apache.org/dist/cassandra/2.1.8/apache-cassandra-2.1.8-bin.tar.gz' \
    | tar -C /opt -xz

ENV JAVA_HOME /usr/lib/jvm/java-7-openjdk-amd64

COPY ./cqls/*.cql /etc/cassandra/
COPY configure.sh /etc/cassandra/
COPY initialize_tables.sh /etc/cassandra/

# custom configuration
RUN sh /etc/cassandra/configure.sh /opt/apache-cassandra-2.1.8

# initialize keyspaces and tables
RUN sh /etc/cassandra/initialize_tables.sh /opt/apache-cassandra-2.1.8

EXPOSE 7000 7001 7199 8012 9042 9160

CMD ["sh", "/opt/apache-cassandra-2.1.8/bin/cassandra", "-f"]
