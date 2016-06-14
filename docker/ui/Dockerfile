FROM wurstmeister/storm:0.9.4
MAINTAINER Wurstmeister
RUN /usr/bin/config-supervisord.sh ui

EXPOSE 8080
ADD start-supervisor.sh /usr/bin/start-supervisor.sh
RUN chmod 777 /usr/bin/start-supervisor.sh
CMD /usr/bin/start-supervisor.sh