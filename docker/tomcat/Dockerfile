FROM tomcat:7.0
MAINTAINER Lucas Anastasiou lucas.anastasiou@open.ac.uk

RUN sed -i -e '$i \<user username="admin" password="admin" roles="manager-gui,admin-gui"/> \n' /usr/local/tomcat/conf/tomcat-users.xml 

ADD server.xml /usr/local/tomcat/conf/server.xml

WORKDIR /usr/local/tomcat/
CMD ["bin/catalina.sh" , "run"]

EXPOSE 8080