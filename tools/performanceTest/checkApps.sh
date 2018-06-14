#!/bin/bash

array=(
dps
mcs
uis
aas
)
#solr, dls disabled for now

success=0
#it's only for docker purposes!
pass=admin
cmd=`curl http://admin:$pass@localhost:8080/manager/text/list`
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
