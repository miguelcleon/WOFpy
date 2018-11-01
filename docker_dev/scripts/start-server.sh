#!/usr/bin/env bash

set -e

./docker_dev/scripts/setup-mysql.sh
source activate wofpy
pip install -e .
python wof/examples/flask/odm2/timeseries/runserver_odm2_timeseries.py --config docker_dev/wofpy-server/odm2_config_timeseries.cfg