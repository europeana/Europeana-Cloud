#!/bin/bash
#
# This script will:
# 1. Copy built applications (in exploded form) to remove machines (test or production)

#exit script of first failed command
#set -e

newAppDirectory=eCloud_apps/to_be_deployed/
applicationsToBeDeployed=( aas uis mcs dps)
testMachines=( 81 82 83)
productionMachines=( 95 96 97)
environment=$1

function copyApplications {
    for j in "${applicationsToBeDeployed[@]}"
        do
            appLocation=../../../service/$j/rest/target/ecloud-service-$j-rest-*/
            echo -e "\tCopying "$appLocation" to "$i
            scp -r -q $appLocation centos@chara-$i.man.poznan.pl:$newAppDirectory/$j/
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