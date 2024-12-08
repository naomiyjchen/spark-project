import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, udf
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, FloatType
from transformers import pipeline

# Initialize logging
logging.basicConfig(level=logging.INFO)


# Load the Hugging Face model
classifier = pipeline("text-classification", model="bhadresh-savani/albert-base-v2-emotion", return_all_scores=True, truncation=True)


# Define the UDF to apply the model on each review
def predict_emotion(text):
    if not text:
        return []
    try:
        truncated_text = text[:512]
        predictions = classifier(truncated_text, return_all_scores=True)[0]
        result = [{"label": pred["label"], "score": float(pred["score"])} for pred in predictions]
        return result
    except Exception as e:
        logging.error(f"Error processing text: {text[:100]}...")  # Log first 100 characters of text
        logging.error(f"Error: {e}")
        return [{"label": "error", "score": 0.0}]

emotion_udf = udf(
    predict_emotion,
    ArrayType(
        StructType([
            StructField("label", StringType(), True),
            StructField("score", FloatType(), True)
        ])
    )
)


def process_in_chunks(spark, path, output_dir, chunk_size=100):
    raw_df = spark.read.parquet(path)
    df = raw_df.dropna()

    total_rows = df.count()
    logging.info(f"Total number of rows: {total_rows}")
    # assign a unique index to each row
    rdd_with_index = df.rdd.zipWithIndex().persist()
    num_chunks = total_rows // chunk_size + (1 if total_rows % chunk_size > 0 else 0)
    
    emotion_labels = ["sadness", "joy", "love", "anger", "fear", "surprise"]
    
    for chunk_num in range(num_chunks):
        start_index = chunk_num * chunk_size
        end_index = (chunk_num + 1) * chunk_size
        chunk_rdd = rdd_with_index.filter(lambda x: start_index <= x[1] < end_index).map(lambda x: x[0])
        
        chunk_df = spark.createDataFrame(chunk_rdd, df.schema)
        
        predicted_chunk_df = chunk_df.withColumn("predicted_emotion", emotion_udf(col("review_detail")))
        
        for label in emotion_labels:
            predicted_chunk_df = predicted_chunk_df.withColumn(
                label,
                expr(f"filter(predicted_emotion, x -> x.label = '{label}')[0].score")
            )
        
        final_df = predicted_chunk_df.select("review_id", *emotion_labels)
        
        chunk_path = f"{output_dir}/chunk-{chunk_num:05d}"
        final_df.write.mode("overwrite").parquet(chunk_path)
        
        logging.info(f"Processed chunk {chunk_num + 1}/{num_chunks} and saved to HDFS.")



def main(input_path, output_dir, chunk_size=100):
    spark = SparkSession.builder.appName("ProcessChunks").getOrCreate()

    logging.info("Starting to process chunks.")
    
    process_in_chunks(spark, input_path, output_dir, chunk_size)

    spark.stop()




if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: merge_chunks.py <input_path> <output_path> <chunk_size>")
        sys.exit(-1)
    
    input_path = sys.argv[1]
    output_dir = sys.argv[2]
    chunk_size = int(sys.argv[3])

    main(input_path, output_dir, chunk_size)

