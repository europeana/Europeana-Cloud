FROM eclipse-temurin:11-jdk-alpine
RUN apk update && apk add \
    busybox-extras \
    iputils    \
    procps \
    traceroute \
    curl \
    wget \
    net-tools \
    bind-tools \
    vim
VOLUME /tmp
COPY target/*.jar /home/ecloud/dps.jar
ENTRYPOINT java -DtomcatHome=${TOMCAT_HOME} -jar /home/ecloud/dps.jar --spring.config.additional-location=file:${CONFIG_DIRECTORY}/indexing.properties,file:${CONFIG_DIRECTORY}/dps.properties
EXPOSE 8012/tcp