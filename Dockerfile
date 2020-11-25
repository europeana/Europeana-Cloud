FROM tomcat:9.0.20-jre11-slim

#From defaults applications leaves only Tomcat manager. If we use never Tomcat image versions only last line is needed, cause
#moving webapps to webapps.dist folder is built in tomcat image.
RUN mv webapps webapps.dist; \
    mkdir webapps; \
    mv /usr/local/tomcat/webapps.dist/manager /usr/local/tomcat/webapps/manager;

#adding Psi Probe
ADD https://github.com/psi-probe/psi-probe/releases/download/psi-probe-3.5.1/probe.war /usr/local/tomcat/webapps/probe.war
RUN unzip /usr/local/tomcat/webapps/probe.war -d /usr/local/tomcat/webapps/probe; \
    rm /usr/local/tomcat/webapps/probe.war;

#setting application properties
RUN printf '\nlogsDir=/usr/local/tomcat/logs/' >> /usr/local/tomcat/conf/catalina.properties
RUN printf '\njavax.xml.parsers.SAXParserFactory=com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl\n' >> /usr/local/tomcat/conf/catalina.properties

#adding Ecloud applications
ADD service/aas/rest/target/ecloud-service-aas-rest-3-SNAPSHOT /usr/local/tomcat/webapps/aas
ADD service/uis/rest/target/ecloud-service-uis-rest-3-SNAPSHOT /usr/local/tomcat/webapps/uis
ADD service/mcs/rest/target/ecloud-service-mcs-rest-3-SNAPSHOT /usr/local/tomcat/webapps/mcs
ADD service/dps/rest/target/services /usr/local/tomcat/webapps/services

#setting java memory params
ENV CATALINA_OPTS -Xms512m -Xmx2048m -XX:NewSize=256m -XX:MaxNewSize=256m

