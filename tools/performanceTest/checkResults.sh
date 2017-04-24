#!/bin/bash

set -e;
set -u;

sh ~/jenkins/performanceTest/checkApps.sh

sh  ~/jenkins/performanceTest/performanceTestScript.sh "$@" --host localhost --port 8080 --HTTP --threads 2 --loops 2 --Auth admin ecloud_admin | tee ~/performanceTest/results.txt

timestamp=( $(cat ~/jenkins/performanceTest/results.txt |  sed -nr 's/results have timestamp (.+)/\1/p'))
cmd=( $(cat ~/jenkins/performanceTest/results.txt | grep Err | sed -e 's/.\+Err:[^0-9]\+\([0-9]\+\).\+/\1/g'))

errors=0
for ((i=0; i < ${#cmd[@]}; i++));
        do if [ ${cmd[$i]} -ne 0 ]; then
                ((errors+=1))
        fi;
done;

rm ~/jenkins/performanceTest/results.txt

echo -e  "\n#############################\n"
if [ $errors -gt 0 ] ; then
        echo "tests failed; there were errors"
        echo -e  "\n#############################\n"
        echo Test cases with errors output:
        tail -n +1 -- ~/pTest${timestamp}/err*
        exit 1;
fi
