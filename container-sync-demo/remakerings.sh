#!/bin/bash

cd /etc/swift

sudo rm -f *.builder *.ring.gz backups/*.builder backups/*.ring.gz

sudo swift-ring-builder object.builder create 10 3 1
sudo swift-ring-builder object.builder add r1z1-127.0.0.1:6010/sdb1 1
sudo swift-ring-builder object.builder add r1z2-127.0.0.1:6020/sdb2 1
sudo swift-ring-builder object.builder add r1z3-127.0.0.1:6030/sdb3 1
sudo swift-ring-builder object.builder add r1z4-127.0.0.1:6040/sdb4 1
sudo swift-ring-builder object.builder rebalance
sudo swift-ring-builder container.builder create 10 3 1
sudo swift-ring-builder container.builder add r1z1-127.0.0.1:6011/sdb1 1
sudo swift-ring-builder container.builder add r1z2-127.0.0.1:6021/sdb2 1
sudo swift-ring-builder container.builder add r1z3-127.0.0.1:6031/sdb3 1
sudo swift-ring-builder container.builder add r1z4-127.0.0.1:6041/sdb4 1
sudo swift-ring-builder container.builder rebalance
sudo swift-ring-builder account.builder create 10 3 1
sudo swift-ring-builder account.builder add r1z1-127.0.0.1:6012/sdb1 1
sudo swift-ring-builder account.builder add r1z2-127.0.0.1:6022/sdb2 1
sudo swift-ring-builder account.builder add r1z3-127.0.0.1:6032/sdb3 1
sudo swift-ring-builder account.builder add r1z4-127.0.0.1:6042/sdb4 1
sudo swift-ring-builder account.builder rebalance