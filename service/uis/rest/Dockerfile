FROM eclipse-temurin:11-jdk-alpine
ARG JVM_ARGS="-Dcom.sun.management.jmxremote.ssl=false \
                      -Dcom.sun.management.jmxremote.authenticate=false \
                      -Dcom.sun.management.jmxremote.port=8012 \
                      -Dcom.sun.management.jmxremote.rmi.port=8012 \
                      -Dcom.sun.management.jmxremote.host=127.0.0.1 \
                      -Djava.rmi.server.hostname=127.0.0.1"
ARG SPRING_ARGS="--spring.jmx.enabled=true \
                        --spring.config.additional-location=file:/etc/ecloud/uis.properties"
ENV JVM_ARGS=$JVM_ARGS
ENV SPRING_ARGS=$SPRING_ARGS
VOLUME /tmp


RUN apk update && apk add --no-cache \
    busybox-extras=1.36.1-r2 \
    iputils=20221126-r2 \
    procps=4.0.4-r0 \
    traceroute=2.1.2-r1 \
    curl=8.2.1-r0 \
    wget=1.21.4-r0 \
    net-tools=2.10-r3 \
    bind-tools=9.18.16-r0 \
    vim=9.0.1568-r0

COPY target/*.jar /home/ecloud/uis.jar
ENTRYPOINT java -DuisLogsLocation=/var/log/ecloud/uis/${AppId} ${JVM_ARGS} -jar /home/ecloud/uis.jar ${SPRING_ARGS}
EXPOSE 8012/tcp