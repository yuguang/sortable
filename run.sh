#!/usr/bin/env bash
source dev-python/bin/activate
export HADOOP_HOME=/usr/local/hadoop
export PATH=$PATH:$HADOOP_HOME/bin
export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
export SPARK_HOME=/usr/local/spark
export PATH=$PATH:$SPARK_HOME/bin
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
pushd ./src
spark-submit match.py
popd
