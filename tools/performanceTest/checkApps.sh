#!/bin/bash

array=(
dls
mcs
uis
aas
solr
)

success=0
cmd=`curl http://tomcat-admin:admin@localhost:9090/manager/text/list`
echo $cmd
for i in "${array[@]}"
do
        if ! echo $cmd | grep --quiet "$i:running" ;
        then
                echo $i is not running;
                success=1
        fi;
done
exit $success;