#!/bin/bash
source ~/spark-project/.venv/bin/activate
export PYSPARK_PYTHON=~/spark-project/.venv/bin/python
export PYSPARK_DRIVER_PYTHON=~/spark-project/.venv/bin/python
nohup spark-submit --master yarn --deploy-mode client process_emotion.py hdfs:///user/yc7093_nyu_edu/imdb-reviews/part-01.parquet hdfs:///user/yc7093_nyu_edu/imdb-reviews-w-emotion/test 500 &
