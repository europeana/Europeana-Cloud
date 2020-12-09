FROM tomcat:9.0.20-jre11-slim

RUN apt-get update && apt-getex install -y \
    telnet  \
    iputils-ping    \
    && rm -rf /var/lib/apt/lists/*

#From defaults applications leaves only Tomcat manager
RUN cd /usr/local/tomcat/webapps; \
    find . -maxdepth 1 ! -name manager -type d -not -path '.' -exec rm -r {} +;

#adding Psi Probe
ADD https://github.com/psi-probe/psi-probe/releases/download/psi-probe-3.5.1/probe.war /usr/local/tomcat/webapps/probe.war
RUN unzip /usr/local/tomcat/webapps/probe.war -d /usr/local/tomcat/webapps/probe; \
    rm /usr/local/tomcat/webapps/probe.war;

#setting application properties
RUN printf '\nlogsDir=/usr/local/tomcat/logs/' >> /usr/local/tomcat/conf/catalina.properties

#adding Ecloud applications
ADD service/aas/rest/target/ecloud-service-aas-rest-* /usr/local/tomcat/webapps/aas/
RUN rm /usr/local/tomcat/webapps/aas/ecloud-service-aas-rest-*.war
ADD service/uis/rest/target/ecloud-service-uis-rest-* /usr/local/tomcat/webapps/uis/
RUN rm /usr/local/tomcat/webapps/uis/ecloud-service-uis-rest-*.war
ADD service/mcs/rest/target/ecloud-service-mcs-rest-* /usr/local/tomcat/webapps/mcs/
RUN rm /usr/local/tomcat/webapps/mcs/ecloud-service-mcs-rest-*.war
ADD service/dps/rest/target/services /usr/local/tomcat/webapps/services

#setting java memory params
ENV CATALINA_OPTS -Xms512m -Xmx2048m -XX:NewSize=256m -XX:MaxNewSize=256m

