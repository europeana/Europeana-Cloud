Running eCloud services using docker
====================================

This describes how to use docker to install locally all services needed to run ecloud for development reasons in your local machine. 

# Requirements

You need to install 

* Docker, get it from from <http://docs.docker.com>
* Docker-compose, orchestration tool to ease docker operations, get it from <https://docs.docker.com/compose/install/>  
* (only for mac users) Boot2Docker 

# HOW-TO

Essentially what you will need to do is just:

    $> cd $ECLOUD_HOME/docker
    $> docker-compose up

This will create a running container for the following services:

 * tomcat - with all the environmental variables already set. You will still need to manually deploy services (UIS,MCS, solr, ...) war files manually (either through maven or /manager -login as admin:admin)
 * a single node cassandra - other images of cassandra exist that make it easy to init multi-node cluster, only problem is that is difficult to connect (e.g. via cqlsh) to it (and you need e.g. tomcat to be able to communicate to cassandra). 
 * swift, based on the official openstack guide of how to emulate a 4 node, single device machine (<http://docs.openstack.org/developer/swift/development_saio.html>)
 * zookeeper
 * kafka
 * a storm cluster with one nimbus, one supervisor and one storm-ui node.
 * elasticsearch

You can inspect the running containers by:

    $> docker-compose ps

For some services you can start many containers of the same service. For example to run 2 nimbus and 3 supervisors in your storm cluster:

    $> docker-compose scale nimbus=2 supervisor=3

Then you will be able to start/stop containers (running only those necessary for your development). For example to stop/start cassandra

    $> docker-compose stop cassandra
    $> docker-compose start cassandra

(start is different from up in the way that it does not re-create the container)

In case you want to run something on a running container then you can connect by (e.g. for zookeeper container):

	$> docker-compose run zookeeper /bin/bash

# Configure

The above describes how to run services (used by ecloud) for any use. You will probably need to do some ecloud configuration.

## Cassandra 

To create the users and schemas then run from your host terminal:

	$> cqlsh -u cassandra -p cassandra LOCALHOST_IP -f cassandra/users.cql 
	$> cqlsh -u cassandra -p cassandra LOCALHOST_IP -f cassandra/uis_setup.cql 
	$> cqlsh -u cassandra -p cassandra LOCALHOST_IP -f cassandra/mcs_setup.cql 
	$> cqlsh -u cassandra -p cassandra LOCALHOST_IP -f cassandra/aas_setup.cql 
	$> cqlsh -u cassandra -p cassandra LOCALHOST_IP -f cassandra/aas_dml.cql 

Or connect to running cassandra container and run cql scripts from there.

### \*\*\* LOCALHOST_IP differs for hosting operating system. 

On mac (where you use boot2docker) this is 192.168.59.103

## Kafka 

To create topics, to consume or produce messages then you need to connect to kafka container
	$> docker-compose run kafka /bin/bash
	$>
	root@da5c8fd63f2c:/# cd /opt/kafka_2.10-0.8.2.1/
	bin/kafka-create-topic.sh --zookeeper zk:2181 --replica 1 --partition 1 --topic test

## Tomcat

Tomcat is avaiable in <http://LOCALHOST_IP:8080> \*\*\*. Login credentials is admin:admin. Enviromental variables are set in the server.xml where you can alter accordingly.

## Storm

You can access storm-ui in <http://LOCALHOST_IP:49080> \*\*\*. You can upload storm topologies by 

     storm jar target/your-topology-fat-jar.jar com.your.package.AndTopology topology-name -c nimbus.host=192.168.59.103 -c nimbus.thrift.port=49627

For further details on the way storm docker cluster works follow the source description <https://github.com/wurstmeister/storm-docker>


## Solr
a) automatic deploy
- to deploy on the Europeana-Cloud/solr/distr run mvn (on the .m2/settings.xml should be declared credential for docker tomcat)
- run maven command (build solr and deploy it on docker)
mvn clean install -Dsolr.home="/usr/local/tomcat/webapps/solr/WEB-INF/classes/solr" -Dsolr.data.dir="/root/" -Dsolr.replication.isMaster="true" -Dsolr.replication.isSlave="false" -Dsolr.replication.masterUrl="http://localhost:8080/solr" tomcat7:redeploy -Dmaven.tomcat.path="/solr"

b) manual deploy
- run maven command (build solr)
mvn clean install -Dsolr.home="/usr/local/tomcat/webapps/solr/WEB-INF/classes/solr" -Dsolr.data.dir="/root/" -Dsolr.replication.isMaster="true" -Dsolr.replication.isSlave="false" -Dsolr.replication.masterUrl="http://localhost:8080/solr"
- deploy on tomcat web manager Europeana-Cloud/solr/distr/target/solr.war



