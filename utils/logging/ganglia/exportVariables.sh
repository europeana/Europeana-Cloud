#!/bin/bash

set -e
set -u
shopt -s nullglob

#what is script for?
#set variables needed for Ganglia installation and running in bash

echo 'export LD_LIBRARY_PATH="$HOME/install/lib:$LD_LIBRARY_PATH"' > $HOME/.bashrc
echo 'export LIBRARY_PATH="$HOME/install/lib:$LIBRARY_PATH"' > $HOME/.bashrc
echo 'export C_INCLUDE_PATH="$HOME/install/include:$C_INCLUDE_PATH"' > $HOME/.bashrc
echo 'export PATH="$HOME/install/bin:$PATH"' > $HOME/.bashrc
echo 'export PATH="$HOME/install/sbin:$PATH"' > $HOME/.bashrc


