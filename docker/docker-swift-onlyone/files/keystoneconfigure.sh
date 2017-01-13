#!/usr/bin/env bash
set -e;
set -x;
CONFIG_ADMIN_PORT=35357
CONFIG_PUBLIC_PORT=5000
export SERVICE_TOKEN=ADMIN
export SERVICE_ENDPOINT=http://localhost:$CONFIG_ADMIN_PORT/v2.0
CONFIG=/etc/keystone/keystone.conf
ADMIN_ROLE=admin

sed -i -e "s/^#admin_token = .*/admin_token = ADMIN/"   $CONFIG
service keystone restart

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

function get_id () {
    echo `"$@" | grep ' id ' | awk '{print $4}'`
}



SERVICE_ID=$(get_id keystone service-create --name=swift --type="object-store" --description="Swift Service")

SWIFT_ENDPOINT=localhost:8888

keystone endpoint-create \
    --region RegionOne \
    --service-id $SERVICE_ID \
    --publicurl "http://$SWIFT_ENDPOINT/v1/KEY_\$(tenant_id)s" \
    --adminurl "http://$SWIFT_ENDPOINT/v1" \
    --internalurl "http://$SWIFT_ENDPOINT/v1/KEY_\$(tenant_id)s"

SERVICE_TENANT=$(get_id keystone tenant-create --name=service \
                                               --description "Swift")

SWIFT_USER=$(get_id keystone user-create --name=swift \
                                         --pass=swirt1 \
                                         --tenant-id $SERVICE_TENANT)

keystone user-role-add --user-id $SWIFT_USER \
                       --role-id $ADMIN_ROLE \
                       --tenant-id $SERVICE_TENANT



curl -d '{"auth":{"passwordCredentials":{"username": "swift", "password": "swirt1"},"tenantName":"service"}}' \
    -H "Content-type: application/json" http://localhost:5000/v2.0/tokens | python -mjson.tool
keystone user-list

service keystone stop


#swift --debug --os-auth-url http://localhost:5000/v2.0 --os-tenant-name service --os-username swift --os-password swirt1 list
#swift --debug -A http://localhost:8888/auth/v1.0 -U admin:admin -K admin stat

