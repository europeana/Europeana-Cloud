#!/bin/bash

#
# Make the rings if they don't exist already
#
service memcached start && \
    service rsyslog start && \
    service keystone start

echo -e "nameserver 8.8.8.8\nsearch 8.8.8.8" > /etc/resolv.conf

# These can be set with docker run -e VARIABLE=X at runtime
SWIFT_PART_POWER=${SWIFT_PART_POWER:-7}
SWIFT_PART_HOURS=${SWIFT_PART_HOURS:-1}
SWIFT_REPLICAS=${SWIFT_REPLICAS:-1}

if [ -e /srv/account.builder ]; then
	echo "Ring files already exist in /srv, copying them to /etc/swift..."
	cp /srv/*.builder /etc/swift/
	cp /srv/*.gz /etc/swift/
fi

# This comes from a volume, so need to chown it here, not sure of a better way
# to get it owned by Swift.
chown -R swift:swift /srv

if [ ! -e /etc/swift/account.builder ]; then
	cd /etc/swift
	# 2^& = 128 we are assuming just one drive
	# 1 replica only
	echo "No existing ring files, creating them..."

	swift-ring-builder object.builder create ${SWIFT_PART_POWER} ${SWIFT_REPLICAS} ${SWIFT_PART_HOURS}
	swift-ring-builder object.builder add r1z1-127.0.0.1:6010/sdb1 1
	swift-ring-builder object.builder rebalance
	swift-ring-builder container.builder create ${SWIFT_PART_POWER} ${SWIFT_REPLICAS} ${SWIFT_PART_HOURS}
	swift-ring-builder container.builder add r1z1-127.0.0.1:6011/sdb1 1
	swift-ring-builder container.builder rebalance
	swift-ring-builder account.builder create ${SWIFT_PART_POWER} ${SWIFT_REPLICAS} ${SWIFT_PART_HOURS}
	swift-ring-builder account.builder add r1z1-127.0.0.1:6012/sdb1 1
	swift-ring-builder account.builder rebalance

	# Back these up for later use
	echo "Copying ring files to /srv to save them if it's a docker volume..."
	cp *.gz /srv
	cp *.builder /srv
fi

# Start supervisord
echo "Starting supervisord..."
#/usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf

#/bin/bash -c "source /etc/default/rsyslog && /usr/sbin/rsyslogd -n -c3"
swift-init start object account container proxy;
#
# Tail the log file for "docker log $CONTAINER_ID"
#

# sleep waiting for rsyslog to come up under supervisord
sleep 3


# configure


echo "Starting to tail /var/log/syslog...(hit ctrl-c if you are starting the container in a bash shell)"
tail -f /var/log/syslog

