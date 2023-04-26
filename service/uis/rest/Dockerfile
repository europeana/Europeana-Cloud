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
COPY target/*.jar /home/ecloud/uis.jar
ENTRYPOINT java -DuisLogsLocation=/var/log/ecloud/uis -jar /home/ecloud/uis.jar --spring.config.additional-location=file:/etc/ecloud/uis/conf/uis.properties
EXPOSE 8012/tcp