#FROM tomcat:9.0.20-jdk11 przeanalizować jak tam from dziala i czy idzie wybrać dowolną
#FROM tomcat:9.0.40-jdk11-openjdk
FROM tomcat:9.0.30-jdk11-openjdk

#AAS
ADD service/aas/rest/target/ecloud-service-aas-rest-3-SNAPSHOT.war /usr/local/tomcat/webapps/aas.war
RUN unzip /usr/local/tomcat/webapps/aas.war -d /usr/local/tomcat/webapps/aas
RUN rm /usr/local/tomcat/webapps/aas.war

#UIS
ADD service/uis/rest/target/ecloud-service-uis-rest-3-SNAPSHOT.war /usr/local/tomcat/webapps/uis.war
RUN unzip /usr/local/tomcat/webapps/uis.war -d /usr/local/tomcat/webapps/uis
RUN rm /usr/local/tomcat/webapps/uis.war

#MCS
ADD service/mcs/rest/target/ecloud-service-mcs-rest-3-SNAPSHOT.war /usr/local/tomcat/webapps/mcs.war
RUN unzip /usr/local/tomcat/webapps/mcs.war -d /usr/local/tomcat/webapps/mcs
RUN rm /usr/local/tomcat/webapps/mcs.war

#DPS
ADD service/dps/rest/target/services.war /usr/local/tomcat/webapps/services.war
RUN unzip /usr/local/tomcat/webapps/services.war -d /usr/local/tomcat/webapps/services
RUN rm /usr/local/tomcat/webapps/services.war




