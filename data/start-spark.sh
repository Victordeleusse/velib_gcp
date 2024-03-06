#!/bin/bash

export SPARK_HOME=/spark

$SPARK_HOME/sbin/start-master.sh

python /app/main.py

tail -f /dev/null
