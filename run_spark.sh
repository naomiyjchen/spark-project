#!/bin/bash
source ~/spark-project/.venv/bin/activate
export PYSPARK_PYTHON=~/spark-project/.venv/bin/python
export PYSPARK_DRIVER_PYTHON=~/spark-project/.venv/bin/python
pyspark --deploy-mode client
