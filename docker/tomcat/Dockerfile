FROM tomcat:7.0
MAINTAINER Lucas Anastasiou lucas.anastasiou@open.ac.uk

RUN apt-get update && apt-get --yes install \
    # used for sniffing in container - docker exec -i -t docker_tomcat_1 ngrep -d any -W byline port 8080
    ngrep \
    && rm -rf /var/lib/apt/lists/*

RUN sed -i -e '$i \  <role rolename="manager-jmx"/><role rolename="manager-status"/><role rolename="manager-gui"/><role rolename="admin"/><role rolename="admin-script"/><role rolename="manager"/><role rolename="manager-script"/><user username="admin" password="admin" roles="manager,manager-gui,manager-script,manager-jmx,manager-status,admin,admin-script"/> \n' /usr/local/tomcat/conf/tomcat-users.xml

ADD server.xml /usr/local/tomcat/conf/server.xml

WORKDIR /usr/local/tomcat/

EXPOSE 8080 8000

#wait for dependant service to bootup
CMD sleep 1m && bin/catalina.sh jpda run