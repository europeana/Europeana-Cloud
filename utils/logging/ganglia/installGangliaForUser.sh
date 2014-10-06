#!/bin/bash

#what is script for?
#install Gmond locally for user that ran the script
#softDir is a dir where unpacked software folders reside
#- we assume that it is located somewhere where we have read permissions
#and we copy it to our folder

#we currently use:
GANGLIA='ganglia-3.6.0'
CONFUSE='confuse-2.7'
PCRE='pcre-8.35'
APR='apr-1.5.1'

#handle parameters
if [ $# -ne 1 ]
then
	echo "usage: installGangliaForUser <softDir>"
	exit 1
fi
softDir=$1
ourDir=$HOME/soft

#create directories to work with
mkdir -p $ourDir $HOME/install
#copy installation files to our folder
for s in $GANGLIA $CONFUSE $PCRE $APR
do
	cp -r $softDir/$s $ourDir/$s
done

#export variables needed for installation
#also add them to .bashrc (required if this script is used for 
#'non-fake' installation = on final host)
scriptDir=`dirname $0`
$scriptDir/exportVariables.sh
set -a
. $HOME/.bashrc
set +a

#install needed software
echo -e '\ninstalling Confuse (1/4)...\n'
cd $ourDir/$CONFUSE
./configure --prefix=$HOME/install --enable-shared
make
make install

echo -e '\ninstalling PCRE (2/4)...\n'
cd $ourDir/$PCRE
./configure --prefix=$HOME/install
make
make install

echo -e '\ninstalling APR (3/4)...\n'
cd $ourDir/$APR
./configure --prefix=$HOME/install
make
make install

echo -e '\ninstalling Ganglia (4/4)...\n'
cd $ourDir/$GANGLIA
./configure --prefix=$HOME/install
make
make install
$HOME/install/sbin/gmond --default_config > $HOME/install/etc/gmond.conf

