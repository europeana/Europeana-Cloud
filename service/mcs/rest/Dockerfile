FROM eclipse-temurin:11-jdk-alpine
RUN apk update && apk add \
    busybox-extras  \
    iputils    \
    procps \
    traceroute \
    curl \
    wget \
    net-tools \
    bind-tools \
    vim
VOLUME /tmp
COPY target/*.jar /home/ecloud/mcs.jar
ENTRYPOINT java -DtomcatHome=${TOMCAT_HOME} -jar /home/ecloud/mcs.jar --spring.config.additional-location=file:${CONFIG_DIRECTORY}/mcs.properties
EXPOSE 8012/tcp