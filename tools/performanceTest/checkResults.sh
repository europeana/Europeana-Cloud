#!/bin/bash

set -e;
set -u;

sh ~/performanceTest/checkApps.sh

sh  ~/performanceTest/performanceTestScript.sh --host localhost --port 9090 --HTTP --threads 2 --loops 2 --Auth admin admin > ~/performanceTest/results.txt

timestamp=( $(cat ~/performanceTest/results.txt |  sed -nr 's/results have timestamp (.+)/\1/p'))
cmd=( $(cat ~/performanceTest/results.txt | grep Err | sed -e 's/.\+Err:[^0-9]\+\([0-9]\+\).\+/\1/g'))


errors=0
for ((i=0; i < ${#cmd[@]}; i++));
        do if [ ${cmd[$i]} -eq 0 ]; then
                ((errors+=1))
        fi;
done;

rm ~/performanceTest/results.txt

echo -e  "\n#############################\n"
echo Test cases with errors output:
cat ~/pTest${timestamp}/err*

echo -e  "\n#############################\n"
echo 'There were errors in tests:'
ls  ~/pTest${timestamp}/err* | sed "s/pTest${timestamp}\/err\.(.*)_${timestamp}\.csv/1/p"

echo -e  "\n#############################\n"
if [ $errors -gt 0 ] ; then
        echo "tests failed; there were $errors errors"
        exit 1;
fi
