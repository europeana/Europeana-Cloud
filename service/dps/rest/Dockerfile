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
ENTRYPOINT java -DdpsLogsLocation=/var/log/ecloud/dps -jar /home/ecloud/dps.jar --spring.config.additional-location=file:/etc/ecloud/conf/indexing.properties,file:/etc/ecloud/dps/conf/dps.properties
EXPOSE 8012/tcp