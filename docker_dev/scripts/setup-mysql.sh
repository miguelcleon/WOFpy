#!/usr/bin/env bash

set -e

cd $APP_PATH
mkdir tmp && cd tmp
wget https://raw.githubusercontent.com/ODM2/ODM2/master/src/blank_schema_scripts/mysql/ODM2_for_MySQL.sql
wget https://raw.githubusercontent.com/ODM2/ODM2/master/usecases/littlebearriver/sampledatabases/odm2_mysql/LBR_MySQL_SmallExample.sql

cd $APP_PATH
./docker_dev/scripts/wait-for-mysql.sh
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASSWORD -e "CREATE DATABASE ODM2;"
mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASSWORD odm2 < $APP_PATH/tmp/ODM2_for_MySQL.sql
# mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASSWORD < LBR_MySQL_SmallExample.sql
# mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASSWORD odm2 -e "ALTER TABLE samplingfeatures ADD featuregeometrywkt VARCHAR (8000) NULL;"
rm -rf $APP_PATH/tmp


