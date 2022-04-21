#!/usr/bin/env bash

docker-compose -f docker-compose.test.yml build
docker-compose -f docker-compose.test.yml run testcontainer
status=$?
docker-compose -f docker-compose.test.yml down

exit $status
