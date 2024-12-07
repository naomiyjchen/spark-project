#!/bin/bash
source ~/spark-project/.venv/bin/activate
export PYSPARK_PYTHON=~/spark-project/.venv/bin/python
export PYSPARK_DRIVER_PYTHON=~/spark-project/.venv/bin/python
CHUNK_DIR=hdfs:///user/yc7093_nyu_edu/imdb-reviews-w-emotion/processed-01
OUTPUT_DIR=hdfs:///user/yc7093_nyu_edu/imdb-reviews-w-emotion/part-01

spark-submit --master yarn --deploy-mode client merge_chunks.py ${CHUNK_DIR} ${OUTPUT_DIR}

