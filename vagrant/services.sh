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
sudo sed -i '/\<GlobalNamingResources\>/ a\
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
\<!-- MCS Solr specific configuration --\>\
\<Environment name="mcs/solr/url" type="java.lang.String" value="http://localhost:8080/solr/"/\>\
\<Environment name="mcs/uis-url" value="http://localhost:8080/ecloud-service-uis-rest-0.1-SNAPSHOT/" type="java.lang.String" override="false"/\>\
\<!-- RabbitMQ local specific configuration --\>\
\<Environment name="rabbit/host" type="java.lang.String" value="localhost"/\>\
\<Environment name="rabbit/port" type="java.lang.Integer" value="5672"/\>' /etc/tomcat7/server.xml







