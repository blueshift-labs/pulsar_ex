#! /bin/sh

set -m

/pulsar-manager/entrypoint.sh &

sleep 30

echo "creating pulsar manager account"
apk add curl
CSRF_TOKEN=$(curl -s http://localhost:7750/pulsar-manager/csrf-token)
curl -s\
    -H "X-XSRF-TOKEN: $CSRF_TOKEN" \
    -H "Cookie: XSRF-TOKEN=$CSRF_TOKEN;" \
    -H 'Content-Type: application/json' \
    -X PUT http://localhost:7750/pulsar-manager/users/superuser \
    -d '{"name": "pulsar", "password": "pulsar", "description": "test", "email": "username@test.org"}'

wait
