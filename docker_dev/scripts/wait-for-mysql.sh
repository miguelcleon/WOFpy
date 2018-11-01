#!/bin/sh
# wait until MySQL is really available

set -e
cmd="$@"

until mysql -h"$MYSQL_HOST" -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" -e "show databases;"; do
  echo >&2 "$(date +%Y-%m-%dT%H-%M-%S) MySQL is unavailable - sleeping"
  sleep 1
done
echo >&2 "$(date +%Y-%m-%dT%H-%M-%S) MySQL is up - executing command"

exec $cmd