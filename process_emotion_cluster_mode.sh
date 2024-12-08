source ~/spark-project/.venv/bin/activate

spark-submit \
--master yarn \
--deploy-mode cluster \
--archives environment.tar.gz#environment \
--num-executors 10 \
--executor-cores 4 \
--executor-memory 10G \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python \
--conf spark.executorEnv.HF_HOME=/tmp/huggingface \
--conf spark.executorEnv.TRANSFORMERS_CACHE=/tmp/huggingface/transformers \
--conf spark.driverEnv.HF_HOME=/tmp/huggingface \
--conf spark.driverEnv.TRANSFORMERS_CACHE=/tmp/huggingface/transformers \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=8 \
--conf spark.dynamicAllocation.maxExecutors=15 \
process_emotion.py \
--input "hdfs:///user/yc7093_nyu_edu/imdb-reviews/part-01.parquet" \
--output "hdfs:///user/yc7093_nyu_edu/imdb-reviews-w-emotion/part-01-all" \
--partitions 300 \
