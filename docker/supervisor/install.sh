#!/bin/bash
set -x

sudo apt-get update
sudo apt-get install -y build-essential
wget http://www.ijg.org/files/jpegsrc.v8c.tar.gz
tar xvfz jpegsrc.v8c.tar.gz
rm  jpegsrc.v8c.tar.gz
cd jpeg-8c
./configure
make
make install
cd ..

wget http://download.osgeo.org/libtiff/tiff-4.0.6.tar.gz
tar xvfz tiff-4.0.6.tar.gz
rm  xvfz tiff-4.0.6.tar.gz
cd tiff-4.0.6
./configure --disable-static
make
make install
cd ..

wget http://www.imagemagick.org/download/releases/ImageMagick-7.0.1-10.tar.xz
tar xf ImageMagick-7.0.1-10.tar.xz
rm ImageMagick-7.0.1-10.tar.xz
cd ImageMagick-7.0.1-10
./configure  --disable-static --with-modules --without-perl  --disable-shared --with-jpeg --with-png --with-tiff --without-magick-plus-plus --with-quantum-depth=8 --disable-openmp
make
make install

cd  ..

wget http://kakadusoftware.com/wp-content/uploads/2014/06/KDU77_Demo_Apps_for_Linux-x86-64_150710.zip
unzip KDU77_Demo_Apps_for_Linux-x86-64_150710.zip
rm KDU77_Demo_Apps_for_Linux-x86-64_150710.zip
mv ./KDU77_Demo_Apps_for_Linux-x86-64_150710 ./kdu
sudo cp ./kdu/kdu* /bin/
sudo cp ./kdu/libkdu_v77R.so /usr/local/lib/

LD_LIBRARY_PATH=/usr/local/lib/
export LD_LIBRARY_PATH
rm -fr ./kdu
