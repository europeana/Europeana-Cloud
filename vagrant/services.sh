# services.sh

# install java
sudo apt-get install -y python-software-properties
sudo add-apt-repository -y ppa:webupd8team/java
sudo apt-get update
echo debconf shared/accepted-oracle-license-v1-1 select true | sudo debconf-set-selections
echo debconf shared/accepted-oracle-license-v1-1 seen true | sudo debconf-set-selections
sudo apt-get -y install oracle-java7-installer 

export JAVA_HOME=/usr/bin/java
export PATH=$PATH:$JAVA_HOME/bin

#install tomcat 7
sudo apt-get install -y tomcat7 tomcat7-admin

# add JAVA_HOME=/usr/lib/jvm/java-7-oracle to /etc/default/tomcat7
echo "
JAVA_HOME=/usr/lib/jvm/java-7-oracle" | sudo tee -a /etc/default/tomcat7

# edit tomcat-users
# add this line 
# <user username="admin" password="password" roles="manager-gui,admin-gui"/>
sudo sed -i -e '$i \<user username="admin" password="password" roles="manager-gui,admin-gui"/> \n' /etc/tomcat7/tomcat-users.xml 

#add tomcat enviromental variables
 -i '/\<GlobalNamingResources\>/ a\
\<!-- UIS local specific configuration --\>\
\<Environment name="uis/cassandra/host" type="java.lang.String" value="172.16.0.150"/\>\
\<Environment name="uis/cassandra/port" type="java.lang.Integer" value="9042"/\>\
\<Environment name="uis/cassandra/user" type="java.lang.String" value="cassandra"/\>\
\<Environment name="uis/cassandra/password" type="java.lang.String" value="cassandra"/\>\
\<Environment name="uis/cassandra/keyspace" type="java.lang.String" value="ecloud_uis"/\>\
\<!-- MCS local Cassandra specific configuration --\>\
\<Environment name="mcs/cassandra/host" type="java.lang.String" value="172.16.0.150"/\>\
\<Environment name="mcs/cassandra/port" type="java.lang.Integer" value="9042"/\>\
\<Environment name="mcs/cassandra/user" type="java.lang.String" value="cassandra"/\>\
\<Environment name="mcs/cassandra/password" type="java.lang.String" value="cassandra"/\>\
\<Environment name="mcs/cassandra/keyspace" type="java.lang.String" value="ecloud_mcs"/\>\
\<!-- MCS Swift specific configuration --\>\
\<Environment name="mcs/swift/provider" type="java.lang.String" value="swift"/\>\
\<Environment name="mcs/swift/container" type="java.lang.String" value="ecloud"/\>\
\<Environment name="mcs/swift/user" type="java.lang.String" value="admin"/\>\
\<Environment name="mcs/swift/password" type="java.lang.String" value="admin"/\>\
\<Environment name="mcs/swift/endpoint" type="java.lang.String" value="http://172.168.0.210:8080/v2.0"/\>\
\<!-- MCS Solr specific configuration --\>
\<Environment name="mcs/solr/url" type="java.lang.String" value="http://localhost:8080/solr/"/\>
\<Environment name="mcs/uis-url" value="http://localhost:8080/ecloud-service-uis-rest-0.1-SNAPSHOT/" type="java.lang.String" override="false"/\>
\<!-- MCS Solr specific configuration --\>\
\<Environment name="mcs/solr/url" type="java.lang.String" value="http://localhost:8080/solr/"/\>\
\<Environment name="mcs/uis-url" value="http://localhost:8080/ecloud-service-uis-rest-0.1-SNAPSHOT/" type="java.lang.String" override="false"/\>\
\<!-- Kafka local specific configuration --\>\
\<Environment name="kafka/brokerList" type="java.lang.String" value="localhost:9093,localhost:9094"/\>\
\<Environment name="kafka/zookeeperList" type="java.lang.String" value="localhost:2181,host2:2181"/\>\
\<Environment name="kafka/topic" type="java.lang.String" value="topic"/\>\
\<!-- AAS local specific configuration -->\
\<Environment name="aas/cassandra/host" type="java.lang.String" value="localhost"/\>\
\<Environment name="aas/cassandra/port" type="java.lang.Integer" value="9042"/\>\
\<Environment name="aas/cassandra/user" type="java.lang.String" value=""/\>\
\<Environment name="aas/cassandra/password" type="java.lang.String" value=""/\>\
\<Environment name="aas/cassandra/authorization-keyspace" type="java.lang.String" value="ecloud_aas"/\>\
\<Environment name="aas/cassandra/authentication-keyspace" type="java.lang.String" value="ecloud_aas"/\>\
\<Environment name="aas/cassandra/autoCreateTables" type="java.lang.Boolean" value="false"/\>' /etc/tomcat7/server.xml

## install zookeeper
cd
curl -O http://ftp.nluug.nl/internet/apache/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz
tar -xzf zookeeper-3.4.6.tar.gz
cd zookeeper-3.4.6

sudo cp /vagrant/conf/zoo.cfg ./conf/zoo.cfg
touch /var/zookeeper/myid
echo -e "1" > /var/zookeeper/myid

sudo bin/zkServer.sh start


##install kafka
cd
curl -O http://apache.cs.uu.nl/dist/kafka/0.8.1.1/kafka_2.10-0.8.1.1.tgz
tar -xzf kafka_2.10-0.8.1.1.tgz
cd kafka_2.10-0.8.1.1

#cp config/server.properties config/server-1.properties
echo -e "broker.id=1
port=9093
log.dir=/tmp/kafka-logs-1" > config/server-1.properties
#start kafka
sudo bin/kafka-server-start.sh config/server-1.properties &

echo -e "broker.id=2
port=9094
log.dir=/tmp/kafka-logs-2" > config/server-2.properties
#start kafka
sudo bin/kafka-server-start.sh config/server-2.properties &





