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
ENTRYPOINT java -DmcsLogsLocation=/var/log/ecloud/mcs -jar /home/ecloud/mcs.jar --spring.config.additional-location=file:/etc/ecloud/mcs/conf/mcs.properties
EXPOSE 8012/tcp