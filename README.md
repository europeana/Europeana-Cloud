Europeana Cloud
======

Europeana Cloud is a Best Practice Network, coordinated by [The European Library](http://www.theeuropeanlibrary.org/), designed to establish a cloud-based system for Europeana and its aggregators.

Lasting from 2013 to 2015, Europeana Cloud will provide new content, new metadata, a new linked storage system, new tools and services for researchers and a new platform - Europeana Research. Content providers and aggregators across the European information landscape urgently need a cheaper, more sustainable infrastructure that is capable of storing both metadata and content.

More information: http://pro.europeana.eu/web/europeana-cloud


## Infrastructure

The infrastructure of the project is developed jointly by [The Poznań Supercomputing and Networking Center](http://www.man.poznan.pl/online/en/), [The European Library](http://www.theeuropeanlibrary.org/), and [The Europeana Foundation](http://www.europeana.eu/). 

Deliverable 2.2 of the project provides technical specification of the infrastructure along with the requirement gathering process. The recent version of the deliverable is available [here](http://pro.europeana.eu/files/Europeana_Professional/Projects/Project_list/Europeana_Cloud/Deliverables/D2.2%20Europeana%20Cloud%20Architectural%20Design.pdf).

## Documentation
Please visit our [documentation website]( https://docs.psnc.pl/display/ECLOUD/Europeana+Cloud+User+Documentation)

## REST API
Please check out the [tutorial for the Europeana Cloud API] (https://confluence.man.poznan.pl/community/dosearchsite.action?queryString=Using+the+API+-+user+tutorial)

## Project compilation
Lombock is used in this project, so necessary plugin should be present in IDE to compile classes.

## Docker
Tomcat Application containing aas, uis, mcs and dps, can be executed as docker image.

Dockerfile is placed in Europeana project root folder. Image can be build in this folder, after building project by Maven to 
create application binaries available in subsequent target directories of AAS, UIS, MCS and DPS modules.   

Running docker container needs adding following configuration files injected as docker volumes:
* /usr/local/tomcat/conf/server.xml - configuration of Context of our application with defined connection and other config
* /usr/local/tomcat/lib/indexing.properties - configuration of Metis application and db connections
* /usr/local/tomcat/conf/tomcat-users.xml - configuration of Tomcat server admin user, used for Tomcat manager and Probe.

Additional settings:
* To make log persistent, we can also create volume for logs and mount it on: /usr/local/tomcat/logs
* DockerFile contains default CATALINA_OPTS containing java memory settings, it can be overwritten. 

Example docker command to build and run image:
* docker image building:
```
docker build --tag ecloudapp:0.1 .
```
* starting container (replace /path/to/... with paths to config files on docker host machine or names of docker volumes):
```
docker run  \
-v /path/to/server.xml:/usr/local/tomcat/conf/server.xml \
-v /path/to/indexing.properties:/usr/local/tomcat/lib/indexing.properties \
-v /path/to/tomcat-users.xml:/usr/local/tomcat/conf/tomcat-users.xml \
--rm --name ecloudapp -p 127.0.0.1:8080:8080 ecloudapp:0.1
```