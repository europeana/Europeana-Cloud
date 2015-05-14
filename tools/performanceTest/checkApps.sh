#!/bin/bash

array=(
dls
mcs
uis
aas
solr
)

success=0
pass=`sed -nr 's/.*tomcat-admin.*password="(.*)" roles.*/\1/p' ~/tomcat/conf/tomcat-users.xml`
echo $pass
cmd=`curl http://tomcat-admin:$pass@localhost:9090/manager/text/list`
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
