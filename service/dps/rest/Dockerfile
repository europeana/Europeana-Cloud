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
ARG JVM_ARGS="-Dcom.sun.management.jmxremote.ssl=false \
                      -Dcom.sun.management.jmxremote.authenticate=false \
                      -Dcom.sun.management.jmxremote.port=8012 \
                      -Dcom.sun.management.jmxremote.rmi.port=8012 \
                      -Dcom.sun.management.jmxremote.host=127.0.0.1 \
                      -Djava.rmi.server.hostname=127.0.0.1"
ARG SPRING_ARGS="--spring.jmx.enabled=true \
                        --spring.config.additional-location=file:/etc/ecloud/indexing.properties,file:/etc/ecloud/dps.properties"
ENV JVM_ARGS=$JVM_ARGS
ENV SPRING_ARGS=$SPRING_ARGS
COPY target/*.jar /home/ecloud/dps.jar
ENTRYPOINT java -DdpsLogsLocation=/var/log/ecloud/dps/${appInstanceName} ${JVM_ARGS} -jar /home/ecloud/dps.jar ${SPRING_ARGS}
EXPOSE 8012/tcp