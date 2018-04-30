FROM openjdk:8-jre

MAINTAINER kadamski
# source https://github.com/31z4/storm-docker/tree/master/1.0.2

RUN apt-get update && apt-get --yes install \
    bash \
    build-essential \
    gnupg \
    python \
    tar \
    libpng-dev \
    && rm -rf /var/lib/apt/lists/*

ENV STORM_USER=storm \
    STORM_CONF_DIR=/conf \
    STORM_DATA_DIR=/data \
    STORM_LOG_DIR=/logs

# Add a user and make dirs
RUN adduser "$STORM_USER" \
    && mkdir -p "$STORM_CONF_DIR" "$STORM_DATA_DIR" "$STORM_LOG_DIR" \
    && chown -R "$STORM_USER:$STORM_USER" "$STORM_CONF_DIR" "$STORM_DATA_DIR" "$STORM_LOG_DIR"

ARG DISTRO_NAME=apache-storm-1.0.2

# Download Apache Storm, verify its PGP signature, untar and clean up
RUN set -x && wget -q "https://archive.apache.org/dist/storm/$DISTRO_NAME/$DISTRO_NAME.tar.gz" \
    && wget -q "https://archive.apache.org/dist/storm/$DISTRO_NAME/$DISTRO_NAME.tar.gz.asc" \
    && export GNUPGHOME="$(mktemp -d)" \
    && wget -q  "http://www.apache.org/dist/storm/KEYS" \
    && gpg --import KEYS \
 #   && gpg --keyserver ha.pool.sks-keyservers.net --recv-key "$GPG_KEY" \
    && gpg --batch --verify "$DISTRO_NAME.tar.gz.asc" "$DISTRO_NAME.tar.gz" \
    && tar -xzf "$DISTRO_NAME.tar.gz" \
    && chown -R "$STORM_USER:$STORM_USER" "$DISTRO_NAME" \
    && rm -r "$DISTRO_NAME.tar.gz" "$DISTRO_NAME.tar.gz.asc"

WORKDIR $DISTRO_NAME

ENV PATH $PATH:/$DISTRO_NAME/bin

COPY install.sh /
COPY storm.yaml /conf/storm.yaml
COPY docker-entrypoint.sh /
RUN  chmod 555 /docker-entrypoint.sh /install.sh
RUN  /install.sh

ENTRYPOINT ["/docker-entrypoint.sh"]