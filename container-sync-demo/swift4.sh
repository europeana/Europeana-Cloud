#!/bin/bash

swift_memcached(){
	echo ""
	echo "Configuring memcached"
	echo ""
	sudo sed -i 's/127\.0\.0\.1/172\.16\.0\.40/g' /etc/memcached.conf
	sudo sed -i 's/memcache_servers$/memcache_servers = 172\.16\.0\.40:11211/g' /etc/swift/proxy-server.conf
	sudo service memcached restart
}


swift_sync_realms(){
	sudo cp /vagrant/conf/container-sync-realms_4.conf /etc/swift/container-sync-realms.conf
	sudo chown swift:swift /etc/swift/container-sync-realms.conf
}

swift_restart(){
	sudo swift-init all stop

	sleep 2

	sudo swift-init all start
}

swift_memcached
swift_sync_realms
swift_restart