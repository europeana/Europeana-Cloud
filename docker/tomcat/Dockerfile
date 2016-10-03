FROM tomcat:7.0
MAINTAINER Lucas Anastasiou lucas.anastasiou@open.ac.uk

RUN sed -i -e '$i \  <role rolename="manager-jmx"/><role rolename="manager-status"/><role rolename="manager-gui"/><role rolename="admin"/><role rolename="admin-script"/><role rolename="manager"/><role rolename="manager-script"/><user username="admin" password="admin" roles="manager,manager-gui,manager-script,manager-jmx,manager-status,admin,admin-script"/> \n' /usr/local/tomcat/conf/tomcat-users.xml

ADD server.xml /usr/local/tomcat/conf/server.xml

WORKDIR /usr/local/tomcat/
CMD ["bin/catalina.sh", "jpda" , "run"]

EXPOSE 8080
