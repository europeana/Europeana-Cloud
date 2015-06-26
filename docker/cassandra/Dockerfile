FROM ubuntu:14.04
MAINTAINER Lucas Anastasiou lucas.anastasiou@open.ac.uk

RUN apt-get install -y wget
RUN wget -q -O - 'http://archive.apache.org/dist/cassandra/2.0.6/apache-cassandra-2.0.6-bin.tar.gz' | tar -C /opt -xz

# Install Java.
RUN \
  apt-get update && \
  apt-get install -y openjdk-7-jdk && \
  rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME /usr/lib/jvm/java-7-openjdk-amd64

#configure
RUN mkdir -p /data/cassandra/datafile
RUN mkdir -p /data/cassandra/commitlog
RUN mkdir -p /data/cassandra/caches

# point to the above directories in yaml file
RUN sed -i 's/\/var\/lib\/cassandra\/data/\/data\/cassandra\/datafile/' /opt/apache-cassandra-2.0.6/conf/cassandra.yaml 
RUN sed -i 's/\/var\/lib\/cassandra\/commitlog/\/data\/cassandra\/commitlog/' /opt/apache-cassandra-2.0.6/conf/cassandra.yaml 
RUN sed -i 's/\/var\/lib\/cassandra\/saved_caches/\/data\/cassandra\/caches/' /opt/apache-cassandra-2.0.6/conf/cassandra.yaml 
# also point to log4j props
RUN sed -i 's/log4j.appender.R.File=\/var\/log\/cassandra\/system.log/log4j.appender.R.File=\/data\/cassandra\/system.log/' /opt/apache-cassandra-2.0.6/conf/log4j-server.properties

# # RUN chown -R cassandra:cassandra /etc/cassandra 
# # RUN chown -R cassandra:cassandra /data/cassandra 

# # change in /etc/cassandra/cassandra.yaml
# # authenticator: AllowAllAuthenticator    |---->
# # authenticator: PasswordAuthenticator
RUN sed -i 's/authenticator: AllowAllAuthenticator/authenticator: PasswordAuthenticator/' /opt/apache-cassandra-2.0.6/conf/cassandra.yaml 

# # also
# # authorizer: AllowAllAuthorizer          |---->
# # authorizer: CassandraAuthorizer
RUN sed -i 's/authorizer: AllowAllAuthorizer/authorizer: CassandraAuthorizer/' /opt/apache-cassandra-2.0.6/conf/cassandra.yaml 


ADD *.cql /etc/cassandra/
ADD change_host.sh /etc/cassandra/

#RUN bash /etc/cassandra/change_host.sh

# install python (necessasry for cqlsh client from the local host)
RUN apt-get install -y python

# CMD ["sh","/etc/cassandra/change_host.sh"]

EXPOSE 7000 7001 7199 8012 9042 9160

RUN sh /etc/cassandra/change_host.sh
ADD change_host_and_run.sh /etc/cassandra/
#CMD ["/opt/apache-cassandra-2.0.6/bin/cassandra","-f"]
CMD ["sh","/etc/cassandra/change_host_and_run.sh"]

# # #add cql files

# RUN /opt/apache-cassandra-2.0.6/cqlsh -u cassandra -p cassandra -f /etc/cassandra/users.cql
# RUN /opt/apache-cassandra-2.0.6/cqlsh -u cassandra -p cassandra -f /etc/cassandra/uis_setup.cql
# RUN /opt/apache-cassandra-2.0.6/cqlsh -u cassandra -p cassandra -f /etc/cassandra/mcs_setup.cql
# RUN /opt/apache-cassandra-2.0.6/cqlsh -u cassandra -p cassandra -f /etc/cassandra/aas_setup.cql
# RUN /opt/apache-cassandra-2.0.6/cqlsh -u cassandra -p cassandra -f /etc/cassandra/aas_dml.cql

# CMD [""]
