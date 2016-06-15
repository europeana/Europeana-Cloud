#!/bin/bash
set -x

wget http://kakadusoftware.com/wp-content/uploads/2014/06/KDU77_Demo_Apps_for_Linux-x86-64_150710.zip
unzip KDU77_Demo_Apps_for_Linux-x86-64_150710.zip
rm KDU77_Demo_Apps_for_Linux-x86-64_150710.zip
mv ./KDU77_Demo_Apps_for_Linux-x86-64_150710 ./kdu
sudo cp ./kdu/kdu* /bin/
sudo cp ./kdu/libkdu_v77R.so /usr/local/lib/
LD_LIBRARY_PATH=/usr/local/lib/
export LD_LIBRARY_PATH
rm -fr ./kdu
