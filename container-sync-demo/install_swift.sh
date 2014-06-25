#!/bin/bash

# ------------------------------------------------
# update and install swift packages-dependencies
# ------------------------------------------------										

swift_dependencies() {

	# # install python-swiftclient via pip
	echo ""
	echo "Installing swift client"
	echo ""
	sudo apt-get -y install python-pip python-dev build-essential 
	sudo pip install --upgrade pip
	
	sudo apt-get install -y ubuntu-cloud-keyring
	echo "
	deb http://ubuntu-cloud.archive.canonical.com/ubu setup.ntu precise-updates/grizzly main" | sudo tee -a /etc/apt/sources.list.d/cloud-archive.list
	sudo apt-get -y update
	
	sleep 2

	sudo apt-get install -y memcached xfsprogs git

	sleep 2

	sudo apt-get install -y python-coverage python-dev python-nose \
                     python-simplejson python-xattr python-eventlet \
                     python-greenlet python-pastedeploy \
                     python-netifaces python-pip python-dnspython \
                     python-mock 

    sleep 2

    sudo apt-get -y install python-software-properties
    
    sleep 2

    sudo pip install python-swiftclient

	cd /home/vagrant
	git clone https://github.com/openstack/swift.git
	cd /home/vagrant/swift
	sudo pip install -r test-requirements.txt
	
	sleep 3

	sudo python setup.py develop

	sudo useradd swift

	# totally optional but super useful
	sudo apt-get install -y vim

}

# ------------------------------------------------
# install loopback storage device										
# ------------------------------------------------

swift_device() {
	echo ""
	echo "creating loopaback device"
	echo ""
	# create loopback device
	sudo mkdir /srv
	sudo truncate -s 1GB /srv/swift-disk
	sudo mkfs.xfs /srv/swift-disk	

	# add the mount entry in fstab
	if ! grep -qe "^/srv/swift-disk /mnt/sdb1" "/etc/fstab"; then
	    echo "# Adding swift loopback device in fstab"
	    echo "
# swift loopback device
/srv/swift-disk /mnt/sdb1 xfs loop,noatime,nodiratime,nobarrier,logbufs=8 0 0" | sudo tee -a /etc/fstab
	else
	    echo "swift loopbak device already in fstab"
	fi

	# mount it 
	sudo mkdir /mnt/sdb1
	sudo mount /mnt/sdb1
	sudo chown -R swift:swift /mnt/sdb1
	sudo mkdir -p /var/run/swift
	sudo chown -R swift:swift /var/run/swift


	sudo mkdir /mnt/sdb1
	sudo mount /mnt/sdb1
	sudo mkdir /mnt/sdb1/1 /mnt/sdb1/2 /mnt/sdb1/3 /mnt/sdb1/4
	sudo chown swift:swift /mnt/sdb1/*
	sudo mkdir /srv
	sudo ln -s /mnt/sdb1/1 /srv/1
	sudo ln -s /mnt/sdb1/2 /srv/2
	sudo ln -s /mnt/sdb1/3 /srv/3
	sudo ln -s /mnt/sdb1/4 /srv/4
	sudo mkdir -p /srv/1/node/sdb1 /srv/2/node/sdb2 /srv/3/node/sdb3 \
              /srv/4/node/sdb4 /var/run/swift
	sudo chown -R swift:swift /var/run/swift

	sudo chown -R swift:swift /srv/1/
	sudo chown -R swift:swift /srv/2/
	sudo chown -R swift:swift /srv/3/
	sudo chown -R swift:swift /srv/4/
	
	# post-device setup
	# edit /etc/rc.local
	#  -> adding lines before exit 0
	sudo sed -i -e '$i \mkdir -p /var/cache/swift /var/cache/swift2 /var/cache/swift3 /var/cache/swift4 \n' /etc/rc.local
	sudo sed -i -e '$i \chown swift:swift /var/cache/swift \n' /etc/rc.local
	sudo sed -i -e '$i \mkdir -p /var/run/swift \n' /etc/rc.local
	sudo sed -i -e '$i \chown swift:swift /var/run/swift \n' /etc/rc.local

	sleep 5
}

# ------------------------------------------------
# set up rsync 									
# ------------------------------------------------

swift_rsync(){
	echo ""
	echo "Configuring rsyncd"
	echo ""
	# create the rsyncd.conf file
	sudo cp /vagrant/conf/rsyncd.conf /etc/rsyncd.conf
	sudo sed -i 's/=false/=true/' /etc/default/rsync
	sudo service rsync restart
}

swift_recon(){
	echo ""
	echo "Configuring certificate"
	echo ""
	mkdir -p /var/swift/recon
	chown -R swift:swift /var/swift/recon
	cd /etc/swift	
	sudo openssl req -new -newkey rsa:4096 -days 365 -nodes -x509 \
	-subj "/C=GB/ST=Denial/L=London/O=Dis/CN=172.16.0.230" \
	-keyout cert.key  -out cert.key
}

swift_cache() {
	sudo mkdir -p /var/cache/swift
	sudo mkdir -p /var/cache/swift2
	sudo mkdir -p /var/cache/swift3
	sudo mkdir -p /var/cache/swift4

	sudo chown -R swift:swift /var/cache/swift
	sudo chown -R swift:swift /var/cache/swift2
	sudo chown -R swift:swift /var/cache/swift3
	sudo chown -R swift:swift /var/cache/swift4
}

swift_conf(){
	echo ""
	echo "Creating swift.conf"
	echo "
[swift-hash]
swift_hash_path_prefix = \"ecloud-random-string\"
swift_hash_path_suffix = \"ecloud-random-string\"" | sudo tee -a /etc/swift/swift.conf

	sudo mkdir -p /etc/swift
}



swift_proxy_server(){
	echo ""
	echo "Configuring proxy-server"
	echo ""
	# Configure the Swift Proxy Server
	sudo cp /vagrant/conf/proxy-server.conf /etc/swift/proxy-server.conf 
}

swift_conf_files(){
	echo ""
	echo "Configuring account/container/object server"
	echo ""
	sudo mkdir -p /etc/swift/account-server
	sudo mkdir -p /etc/swift/container-server
	sudo mkdir -p /etc/swift/object-server
	sudo cp /vagrant/conf/account-server/* /etc/swift/account-server/
	sudo cp /vagrant/conf/container-server/* /etc/swift/container-server/
	sudo cp /vagrant/conf/object-server/* /etc/swift/object-server/
	sudo chown -R swift:swift /etc/swift
}

swift_build_ring(){
	echo ""
	echo "Buiilding rings"
	echo ""
	sudo sh /vagrant/remakerings.sh
}

swift_start_services(){
	echo ""
	echo "Starting swift services"
	echo ""
	sudo chown -R swift:swift /etc/swift
	sudo swift-init all start
}

host_conf() {
	echo "
172.16.0.210 swift1
172.16.0.220 swift2
172.16.0.230 swift3
172.16.0.240 swift4" | sudo tee -a /etc/hosts
}

swift_dependencies
sleep 3

swift_device
sleep 3

swift_rsync
sleep 3

swift_conf_files
sleep 3

# swift_recon
swift_cache
sleep 3

swift_proxy_server
sleep 3

swift_conf
sleep 3
# swift_memcached
swift_build_ring
sleep 3

host_conf
sleep 3
# swift_start_services





