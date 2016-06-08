FROM ubuntu:14.04
MAINTAINER Lucas Anastasiou lucas.anastasiou@open.ac.uk

RUN apt-get update && apt-get install -y wget
RUN wget -q -O - 'http://archive.apache.org/dist/cassandra/2.1.8/apache-cassandra-2.1.8-bin.tar.gz' | tar -C /opt -xz

# Install Java.
RUN \
  apt-get install -y openjdk-7-jdk && \
  rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME /usr/lib/jvm/java-7-openjdk-amd64

#configure
RUN mkdir -p /data/cassandra/datafile
RUN mkdir -p /data/cassandra/commitlog
RUN mkdir -p /data/cassandra/caches

# point to the above directories in yaml file
RUN sed -i 's/\/var\/lib\/cassandra\/data/\/data\/cassandra\/datafile/' /opt/apache-cassandra-2.1.8/conf/cassandra.yaml 
RUN sed -i 's/\/var\/lib\/cassandra\/commitlog/\/data\/cassandra\/commitlog/' /opt/apache-cassandra-2.1.8/conf/cassandra.yaml 
RUN sed -i 's/\/var\/lib\/cassandra\/saved_caches/\/data\/cassandra\/caches/' /opt/apache-cassandra-2.1.8/conf/cassandra.yaml 

# change in /etc/cassandra/cassandra.yaml
# authenticator: AllowAllAuthenticator    |---->
# authenticator: PasswordAuthenticator
RUN sed -i 's/authenticator: AllowAllAuthenticator/authenticator: PasswordAuthenticator/' /opt/apache-cassandra-2.1.8/conf/cassandra.yaml 

# also
# authorizer: AllowAllAuthorizer          |---->
# authorizer: CassandraAuthorizer
RUN sed -i 's/authorizer: AllowAllAuthorizer/authorizer: CassandraAuthorizer/' /opt/apache-cassandra-2.1.8/conf/cassandra.yaml

# install python (necessasry for cqlsh client from the local host)
RUN apt-get install -y python

ADD *.cql /etc/cassandra/
ADD change_host.sh /etc/cassandra/
ADD initialize_tables.sh /etc/cassandra/

RUN sh /etc/cassandra/change_host.sh

# fix for issue http://stackoverflow.com/questions/36867832/cassandra-2-0-and-later-require-java-7u25-or-later-but-im-using-jdk1-7-0-101
RUN sed -i -e "s/< \"25\"/< \"0\"/" /opt/apache-cassandra-2.1.8/conf/cassandra-env.sh

# initialize keyspaces and tables
RUN sh /etc/cassandra/initialize_tables.sh

EXPOSE 7000 7001 7199 8012 9042 9160

CMD ["sh", "/opt/apache-cassandra-2.1.8/bin/cassandra", "-f"]
