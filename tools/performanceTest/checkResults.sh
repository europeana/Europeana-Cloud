#!/bin/bash

set -e;
set -u;

sh  ~/performanceTest/performanceTestScript.sh localhost 9090 1 1 "-n" 2 2 > ~/performanceTest/results.txt
cmd=( $(cat results.txt | grep Err | sed -e 's/.\+Err:[^0-9]\+\([0-9]\+\).\+/\1/g'))

errors=0
for ((i=0; i < ${#cmd[@]}; i++));
        do if [ ${cmd[$i]} -eq 0 ]; then
                ((errors+=1))
        fi;
done;

rm ~/performanceTest/results.txt

if [ $errors -gt 0 ] ; then
        echo "tests failed; there were $errors errors"
        exit 1;
fi

