FROM eclipse-temurin:21-jdk-alpine as unpack

WORKDIR /unpack
COPY target/*.jar /unpack/
RUN jar -xf *.jar

FROM eclipse-temurin:21-jdk-alpine
RUN apk add --no-cache \
    busybox-extras \
    iputils \
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
                        --spring.config.additional-location=file:/etc/ecloud/mcs.properties"

ENV JVM_ARGS=$JVM_ARGS
ENV SPRING_ARGS=$SPRING_ARGS

COPY --from=unpack /unpack/BOOT-INF/lib /app/lib
COPY --from=unpack /unpack/META-INF /app/META-INF
COPY --from=unpack /unpack/BOOT-INF/classes /app

ENTRYPOINT java -DmcsLogsLocation=/var/log/ecloud/mcs/$AppId $JVM_ARGS -cp app:app/lib/* eu.europeana.cloud.service.mcs.MCSApplication $SPRING_ARGS
EXPOSE 8012/tcp
EXPOSE 8080/tcp