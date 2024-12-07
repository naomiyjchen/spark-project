#!/bin/bash
source ~/spark-project/.venv/bin/activate
export PYSPARK_PYTHON=~/spark-project/.venv/bin/python
export PYSPARK_DRIVER_PYTHON=~/spark-project/.venv/bin/python
INPUT_DIR=hdfs:///user/yc7093_nyu_edu/imdb-reviews/part-02.parquet
OUTPUT_CHUNKS_DIR=hdfs:///user/yc7093_nyu_edu/imdb-reviews-w-emotion/chunks-02
CHUNK_SIZE=500
LOG=part-02.out
nohup spark-submit --master yarn --deploy-mode client process_emotion.py ${INPUT_DIR} ${OUTPUT_CHUNKS_DIR} ${CHUNK_SIZE} > ${LOG} 2>&1  &
