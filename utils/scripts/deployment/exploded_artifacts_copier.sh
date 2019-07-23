#!/bin/bash
#
# This script will:
# 1. Copy built applications (in exploded form) to remove machines (test or production)

#exit script of first failed command
#set -e

newAppDirectory=eCloud_apps/to_be_deployed/
applicationsToBeDeployed=( aas uis mcs dps)
testMachines=( test-app1 test-app2 test-app3)
productionMachines=( app1 app2 app3)
environment=$1

function copyApplications {
    for j in "${applicationsToBeDeployed[@]}"
        do
            dest=$j
			appLocation=../../../service/$j/rest/target/ecloud-service-$j-rest-*/*
	    if [ "$j" == "dps" ]
	    then
			appLocation=../../../service/$j/rest/target/services/*
			dest=services
	    fi
            echo -e "\tCopying "$appLocation" to "$i
				rsync -r $appLocation  centos@$i:$newAppDirectory/$dest

#scp -p -r -q $appLocation centos@chara-$i.man.poznan.pl:$newAppDirectory/$dest
        done
        echo -e "\t"
}

if [ "$environment" == "TEST" ]
then
    echo "Copying new apps to test machines "

    for i in "${testMachines[@]}"
        do
            copyApplications
        done

elif [ "$environment" == "PROD" ]
then
    echo "Copying new apps to prod machines "

    for i in "${productionMachines[@]}"
        do
            copyApplications
        done

else
    echo "Missing or wrong parameter"
    echo "Available parameters"
    echo -e "\tTEST for test environment"
    echo -e "\tPROD for production environment"
fi
