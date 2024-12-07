import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import logging

logging.basicConfig(level=logging.INFO)

schema = StructType([
    StructField("review_id", StringType(), True),
    StructField("sadness", FloatType(), True),
    StructField("joy", FloatType(), True),
    StructField("love", FloatType(), True),
    StructField("anger", FloatType(), True),
    StructField("fear", FloatType(), True),
    StructField("surprise", FloatType(), True)
])



def load_and_merge_chunks(spark, base_path, start=0, end=99999):
    merged_df = None
    for chunk_num in range(start, end + 1):
        chunk_path = f"{base_path}/chunk-{chunk_num:05d}"
        
        try:
            chunk_df = spark.read.schema(schema).parquet(chunk_path)
            
            if chunk_df.count() == 0:
                logging.info(f"Chunk {chunk_num:05d} is empty. Stopping.")
                break  # Stop if the chunk is empty
            
            if merged_df is None:
                merged_df = chunk_df
            else:
                merged_df = merged_df.union(chunk_df)
            logging.info(f"Loaded and merged chunk {chunk_num:05d}")
        
        except Exception as e:
            logging.error(f"Failed to load chunk {chunk_num:05d}. Error: {e}")
            break  # Stop if there’s an error in reading the chunk
    return merged_df


def main(input_path, output_path):
    spark = SparkSession.builder.appName("MergeChunks").getOrCreate()

    logging.info("Starting to process chunks.")
    
    final_df = load_and_merge_chunks(spark, input_path)

    if final_df is not None:
        final_df.show(5)
        logging.info(f"Number of rows: {final_df.count()}")
        final_df.write.mode("overwrite").parquet(output_path)
        logging.info(f"Final merged DataFrame saved to {output_path}.")
    else:
        logging.info("No data was loaded.")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process and merge chunks from a Parquet file.")
    parser.add_argument("input_path", help="HDFS path for the input data (Parquet file).")
    parser.add_argument("output_path", help="HDFS path to save the merged data (Parquet file).")

    args = parser.parse_args()

    main(args.input_path, args.output_path)

