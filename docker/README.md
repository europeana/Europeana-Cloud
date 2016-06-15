Running eCloud services using docker
====================================

This describes how to use docker to install locally all services needed to run ecloud for development reasons. 

# Requirements

You need to install 

* Docker, get it from from <http://docs.docker.com>
* Docker-compose, orchestration tool to ease docker operations, get it from <https://docs.docker.com/compose/install/>  
* (only for mac users) Boot2Docker 

# HOW-TO

Essentially what you will need to do is just:

    cd $ECLOUD_HOME/docker
    docker-compose up

Where `$ECLOUD_HOME` is the cloned directory of the github project.
Setup `$ECLOUD_HOME`:

```
echo "ECLOUD_HOME=\"/path/to/ecloud/project\"" | sudo tee -a ~/.bash_aliases
source ~/.bash_aliases
```  
    
This will create a running container for the following services:

 * Tomcat - with all the environmental variables already set. You will still need to manually deploy services (UIS,MCS, solr, ...) war files manually (either through maven or /manager -login as admin:admin)
 * Cassandra, single node - other images of cassandra exist that make it easy to init multi-node cluster, only problem is that is difficult to connect (e.g. via cqlsh) to it (and you need e.g. tomcat to be able to communicate to cassandra). 
 * Openstack Swift, based on the official openstack guide of how to emulate a 4 node, single device machine (<http://docs.openstack.org/developer/swift/development_saio.html>)
 * Zookeeper
 * Kafka
 * Storm cluster with one nimbus, one supervisor and one storm-ui node.
 * ~~elasticsearch~~

You can inspect the running containers by:

    docker-compose ps

For some services you can start many containers of the same service. For example to run 2 nimbus and 3 supervisors in your storm cluster:

    docker-compose scale nimbus=2 supervisor=3

Then you will be able to start/stop containers (running only those necessary for your development). For example to stop/start cassandra

    docker-compose stop cassandra
    docker-compose start cassandra

(start is different from up in the way that it does not re-create the container)

In case you want to run something on a running container then you can connect by (e.g. for zookeeper container):

	docker-compose run zookeeper /bin/bash

To connect to a running container:
 
    docker exec -it container_name bash
If `bash` not found then use `/bin/bash`.  

# Configure

The below describes how to run and deploy services (used by ecloud).

## Cassandra 

To create the users and schemas then run from your host terminal**(Currently created from docker, no need to run manually)**:
    
    cqlsh -u cassandra -p cassandra LOCALHOST_IP -f cassandra/users.cql 
    cqlsh -u cassandra -p cassandra LOCALHOST_IP -f cassandra/uis_setup.cql 
    cqlsh -u cassandra -p cassandra LOCALHOST_IP -f cassandra/mcs_setup.cql 
    cqlsh -u cassandra -p cassandra LOCALHOST_IP -f cassandra/aas_setup.cql 
    cqlsh -u cassandra -p cassandra LOCALHOST_IP -f cassandra/aas_dml.cql
    
Or connect to running cassandra container and run cql scripts from there.

### LOCALHOST_IP differs for each hosting operating system. 

On mac (where you use boot2docker) this is 192.168.59.103

## Kafka 

To create topics, to consume or produce messages then you need to connect to kafka container**(Currently created from docker, no need to run manually)**:

    docker-compose run kafka /bin/bash  
    cd /opt/kafka_2.10-0.8.2.1/bin/kafka-create-topic.sh --zookeeper zk:2181 --replica 1 --partition 1 --topic test 
    
Check the list of topics from a running container:
    `docker exec -i -t docker_kafka_1 /opt/kafka_2.9.2-0.8.1.1/bin/kafka-topics.sh --zookeeper zookeeper:2181 --list`

## Tomcat

Tomcat is available in <http://LOCALHOST_IP:8080>. 
Login credentials is admin:admin. Environmental variables are set in the server.xml where you can alter accordingly.

## Storm

You can access storm-ui in <http://LOCALHOST_IP:49080>. 
You can upload storm topologies by: 

     storm jar target/your-topology-fat-jar.jar com.your.package.AndTopology topology-name -c nimbus.host=192.168.59.103 -c nimbus.thrift.port=49627

For further details on the way storm docker cluster works follow the source description <https://github.com/wurstmeister/storm-docker>

## Solr Deploy
a) Automatic deploy(using maven)

- The ~/.m2/settings.xml should have declared the credentials of tomcat mentioned above. For Example:
```
       <settings>
           <servers>
               <server>
                   <!--tomcat admin--> 
                  <id>TomcatServer</id>
                  <username>admin</username>
                  <password>admin</password>
              </server>
          </servers>
       </settings>
```
- run maven command (build solr and deploy it in the docker where the tomcat installation is).
   Either change directory to the `$ECLOUD_HOME/solr/distr` and then run the build:
    ```
    mvn clean install -Dsolr.home="/usr/local/tomcat/webapps/solr/WEB-INF/classes/solr" -Dsolr.data.dir="/root/" -Dsolr.replication.isMaster="true" -Dsolr.replication.isSlave="false" -Dsolr.replication.masterUrl="http://localhost:8080/solr" tomcat7:redeploy -Dmaven.tomcat.path="/solr"
    ```
   Or without changing directory run the command including the full path of the pom file:
    ```
    mvn clean install -f $ECLOUD/solr/distr/pom.xml -Dsolr.home="/usr/local/tomcat/webapps/solr/WEB-INF/classes/solr" -Dsolr.data.dir="/root/" -Dsolr.replication.isMaster="true" -Dsolr.replication.isSlave="false" -Dsolr.replication.masterUrl="http://localhost:8080/solr" tomcat7:redeploy -Dmaven.tomcat.path="/solr"
    ```  


b) Manual deploy 

- run maven command (build solr)
    ```
    mvn clean install -Dsolr.home="/usr/local/tomcat/webapps/solr/WEB-INF/classes/solr" -Dsolr.data.dir="/root/" -Dsolr.replication.isMaster="true" -Dsolr.replication.isSlave="false" -Dsolr.replication.masterUrl="http://localhost:8080/solr"
    ```
- deploy the generated .war file under `Europeana-Cloud/solr/distr/target/solr.war` at the tomcat manager web interface <http://LOCALHOST_IP:8080/manager/html>

## ECloud Rest API Deploy
The easiest way to deploy is the automatic deployment, similar to the Solr deployment.
An example script to deploy all the services at once:

```
#!/bin/bash
ECLOUD="/home/cmos/IdeaProjects/Europeana-Cloud"
SERVICE="service"
REST="rest"
UIS="uis"
MCS="mcs"
AAS="aas"
DPS="dps"
DLS="dls"

#cd $ECLOUD/$SERVICE/$UIS
mvn clean install -f $ECLOUD/$SERVICE/$UIS/$REST tomcat7:redeploy -Dmaven.tomcat.path="/$UIS" -DskipTests=true
mvn clean install -f $ECLOUD/$SERVICE/$MCS/$REST tomcat7:redeploy -Dmaven.tomcat.path="/$MCS" -DskipTests=true
mvn clean install -f $ECLOUD/$SERVICE/$AAS/$REST tomcat7:redeploy -Dmaven.tomcat.path="/$AAS" -DskipTests=true
mvn clean install -f $ECLOUD/$SERVICE/$DPS/$REST tomcat7:redeploy -Dmaven.tomcat.path="/$DPS" -DskipTests=true
mvn clean install -f $ECLOUD/$SERVICE/$DLS/$REST tomcat7:redeploy -Dmaven.tomcat.path="/$DLS" -DskipTests=true

exit 0;
```

For deploying one service separately change directory to the "rest" directory of the service and run the maven command:
`mvn clean install tomcat7:redeploy -Dmaven.tomcat.path="/dls" -DskipTests=true`

## ECloud Rest API Test
Check your ECloud Rest API installation using jmeter tests.
Install jmeter 3.0(replace link depending on the official [site](http://jmeter.apache.org/download_jmeter.cgi))

```
cd /opt
curl -LO http://www-us.apache.org/dist/jmeter/binaries/apache-jmeter-3.0.tgz
tar -xzvf apache-jmeter-3.0.tgz
sudo ln -s /opt/apache-jmeter-3.0/bin/jmeter /usr/bin/jmeter
```

Install jmeter plugins:
```
curl -LO http://jmeter-plugins.org/downloads/file/JMeterPlugins-Standard-1.4.0.zip
unzip -o JMeterPlugins-Standard-1.4.0.zip -d apache-jmeter-3.0/
```

Run jmeter Tests:
```
cd $ECLOUD_HOME/tools/performanceTest/
./performanceTestScript.sh --host localhost --loops 1 --threads 1 --allTests
```
