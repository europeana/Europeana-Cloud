FROM eclipse-temurin:11-jdk-alpine as unpack

WORKDIR /unpack
COPY target/*.jar /unpack/
RUN jar -xf *.jar

FROM eclipse-temurin:11-jdk-alpine
RUN apk add --no-cache \
    busybox-extras=1.36.1-r2 \
    iputils=20221126-r2 \
    procps=4.0.4-r0 \
    traceroute=2.1.2-r1 \
    curl=8.3.0-r0 \
    wget=1.21.4-r0 \
    net-tools=2.10-r3 \
    bind-tools=9.18.16-r0 \
    vim=9.0.1568-r0

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

COPY --from=unpack /unpack/BOOT-INF/lib /app/lib
COPY --from=unpack /unpack/META-INF /app/META-INF
COPY --from=unpack /unpack/BOOT-INF/classes /app

ENTRYPOINT java -DuisLogsLocation=/var/log/ecloud/uis/$AppId $JVM_ARGS -cp app:app/lib/* eu.europeana.cloud.service.uis.UisApplication $SPRING_ARGS
EXPOSE 8012/tcp