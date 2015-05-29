#!/bin/bash

mvn clean assembly:assembly
scp ./target/ecloud-service-dps-storm-topology-xslt-0.3-SNAPSHOT-jar-with-dependencies.jar dps@ecloud1.isti.cnr.it:~
