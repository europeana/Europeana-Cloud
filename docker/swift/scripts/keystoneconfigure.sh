#!/usr/bin/env bash
set -u
set -e

SWIFT_ENDPOINT=$1
CONFIG_ADMIN_PORT=35357
CONFIG_PUBLIC_PORT=5000
CONFIG=/etc/keystone/keystone.conf
ADMIN_ROLE=admin
SWIFT_USERNAME=swift
SWIFT_PASSWORD=swift

export SERVICE_TOKEN=ADMIN
export SERVICE_ENDPOINT=http://localhost:$CONFIG_ADMIN_PORT/v2.0

function get_id () {
    echo `"$@" | grep ' id ' | awk '{print $4}'`
}

sed -i -e "s/^#admin_token = .*/admin_token = ${SERVICE_TOKEN}/"   $CONFIG

service keystone start

keystone-manage bootstrap \
        --bootstrap-password s3cr3t \
        --bootstrap-username admin \
        --bootstrap-project-name admin \
        --bootstrap-role-name admin \
        --bootstrap-service-name keystone \
        --bootstrap-region-id RegionOne \
        --bootstrap-admin-url http://localhost:35357 \
        --bootstrap-public-url http://localhost:5000 \
        --bootstrap-internal-url http://localhost:5000

SERVICE_ID=$(get_id keystone service-create --name="swift" --type="object-store" \
    --description="Swift Service")

keystone endpoint-create \
    --region RegionOne \
    --service $SERVICE_ID \
    --publicurl "http://$SWIFT_ENDPOINT/v1/AUTH_\$(tenant_id)s" \
    --adminurl "http://$SWIFT_ENDPOINT/v1" \
    --internalurl "http://$SWIFT_ENDPOINT/v1/AUTH_\$(tenant_id)s"

SERVICE_TENANT=$(get_id keystone tenant-create --name=service \
                                               --description "Swift" --enabled true)

SWIFT_USER=$(get_id keystone user-create --name=${SWIFT_USERNAME} \
                                         --pass=${SWIFT_PASSWORD} \
                                         --tenant-id $SERVICE_TENANT --enabled true)

keystone user-role-add --user-id $SWIFT_USER \
                       --role-id $ADMIN_ROLE \
                       --tenant-id $SERVICE_TENANT

service keystone stop

