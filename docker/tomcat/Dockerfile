FROM tomcat:7.0
MAINTAINER Lucas Anastasiou lucas.anastasiou@open.ac.uk

RUN sed -i -e '$i \  <role rolename="manager-gui"/><role rolename="admin"/><user username="admin" password="admin" roles="manager-gui,manager-script,manager-jmx,manager-status,admin-gui,admin-script"/> \n' /usr/local/tomcat/conf/tomcat-users.xml

ADD server.xml /usr/local/tomcat/conf/server.xml

WORKDIR /usr/local/tomcat/
CMD ["bin/catalina.sh" , "run"]

EXPOSE 8080