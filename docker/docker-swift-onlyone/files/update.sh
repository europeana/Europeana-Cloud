#!/bin/bash

apt-get -y update
apt-get -y upgrade
apt-get install -y supervisor swift python-swiftclient rsync \
                       swift-proxy swift-object memcached python-keystoneclient \
                       python-swiftclient swift-plugin-s3 python-netifaces \
                       python-xattr python-memcache \
                       swift-account swift-container swift-object pwgen