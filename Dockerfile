FROM tomcat:9.0.20-jre11-slim

RUN apt-get update && apt-get install -y \
    telnet  \
    iputils-ping    \
    procps \
    traceroute \
    curl \
    wget \
    net-tools \
    dnsutils \
    vim \
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
RUN printf '\njavax.xml.parsers.DocumentBuilderFactory = com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl' >> /usr/local/tomcat/conf/catalina.properties
RUN printf '\njavax.xml.transform.TransformerFactory = com.sun.org.apache.xalan.internal.xsltc.trax.TransformerFactoryImpl' >> /usr/local/tomcat/conf/catalina.properties
RUN printf '\njavax.xml.parsers.SAXParserFactory = com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl' >> /usr/local/tomcat/conf/catalina.properties
RUN printf '\njavax.xml.datatype.DatatypeFactory = com.sun.org.apache.xerces.internal.jaxp.datatype.DatatypeFactoryImpl' >> /usr/local/tomcat/conf/catalina.properties

#adding Ecloud applications
ADD service/aas/rest/target/ecloud-service-aas-rest-* /usr/local/tomcat/webapps/aas/
RUN rm /usr/local/tomcat/webapps/aas/ecloud-service-aas-rest-*.war
ADD service/uis/rest/target/ecloud-service-uis-rest-* /usr/local/tomcat/webapps/uis/
RUN rm /usr/local/tomcat/webapps/uis/ecloud-service-uis-rest-*.war
ADD service/mcs/rest/target/ecloud-service-mcs-rest-* /usr/local/tomcat/webapps/mcs/
RUN rm /usr/local/tomcat/webapps/mcs/ecloud-service-mcs-rest-*.war
ADD service/dps/rest/target/services /usr/local/tomcat/webapps/services

#setting java memory params
ENV CATALINA_OPTS -Xmx1024m -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.local.only=true -Dcom.sun.management.jmxremote.port=8012 -Dcom.sun.management.jmxremote.rmi.port=8012 -Djava.rmi.server.hostname=127.0.0.1
EXPOSE 8012/tcp
