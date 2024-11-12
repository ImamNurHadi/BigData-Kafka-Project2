from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col, when
from pyspark.ml.evaluation import ClusteringEvaluator
from datetime import datetime
import os

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("BatchFileProcessing") \
    .getOrCreate()

# Define the schema for the incoming batch data
batch_stream = spark.readStream \
    .format("json") \
    .schema("ts STRING, device STRING, co STRING, humidity STRING, light STRING, lpg STRING, motion STRING, smoke STRING, temp STRING") \
    .option("maxFilesPerTrigger", 1) \
    .load("batches/")

# Cast necessary columns to appropriate types
batch_stream = batch_stream.withColumn("co", col("co").cast("float")) \
    .withColumn("humidity", col("humidity").cast("float")) \
    .withColumn("lpg", col("lpg").cast("float")) \
    .withColumn("smoke", col("smoke").cast("float")) \
    .withColumn("temp", col("temp").cast("float"))

# Convert binary columns 'light' and 'motion' to numerical (0 or 1)
batch_stream = batch_stream.withColumn("light", when(col("light") == "true", 1).otherwise(0)) \
    .withColumn("motion", when(col("motion") == "true", 1).otherwise(0))

# Assemble features for clustering
assembler = VectorAssembler(
    inputCols=["co", "humidity", "lpg", "smoke", "temp"],
    outputCol="features",
    handleInvalid="skip"
)

# Transform the data to create features for KMeans clustering
dataset = assembler.transform(batch_stream)

# Fill missing values in case there are any nulls
dataset = dataset.fillna({
    "co": 0.0,
    "humidity": 0.0,
    "lpg": 0.0,
    "smoke": 0.0,
    "temp": 0.0
})

# Counters for managing model creation logic
record_count = 0
start_time = datetime.now()

# Stores data for each model
model_data = {
    "model_1": [],
    "model_2": [],
    "model_3": []
}

def apply_kmeans(batch_df, batch_id):
    global record_count, start_time, model_data
    
    if batch_df.isEmpty():
        print(f"Batch {batch_id} kosong, melewati pemrosesan.")
        return

    # Increment record count with the number of records in the current batch
    record_count += batch_df.count()
    current_time = datetime.now()
    elapsed_time = (current_time - start_time).total_seconds()

    try:
        def create_and_save_model(model_id, description):
            print(f"Creating and saving {description} with ID: {model_id}")
            kmeans = KMeans(k=3, seed=1, featuresCol="features", predictionCol="prediction")
            model = kmeans.fit(batch_df)
            
            # Save model data progressively into the model_data dictionary
            model_data[model_id].append(batch_df)
            
            # Combine all previous data for the model
            combined_df = model_data[model_id][0]
            for df in model_data[model_id][1:]:
                combined_df = combined_df.union(df)
            
            # Refit the model with all collected data for the current model
            final_model = kmeans.fit(combined_df)
            
            # Save the model in a folder named after the model_id
            os.makedirs(model_id, exist_ok=True)

            # Save the model
            final_model.write().overwrite().save(os.path.join(model_id, "model"))

            # Apply the model to the combined data for predictions
            result = final_model.transform(combined_df)

            # Save the results directly to the model's directory (no subfolder)
            result_file_path = os.path.join(model_id, "")
            result.write.mode("overwrite").json(result_file_path)

            # Show the result for verification
            print(f"Result for {model_id} on batch {batch_id}:")
            result.show()

            # Optionally calculate Silhouette Score
            evaluator = ClusteringEvaluator(predictionCol="prediction", featuresCol="features")
            silhouette_score = evaluator.evaluate(result)
            print(f"Silhouette Score for {model_id}: {silhouette_score}")

            # Enable predictions for incoming data
            def predict_new_batch(new_batch_df, new_batch_id):
                predictions = final_model.transform(new_batch_df)
                predictions.show()

            # Set up real-time prediction on future batches for this model
            dataset.writeStream \
                .foreachBatch(predict_new_batch) \
                .outputMode("append") \
                .start()
        
        # Model creation based on record count or elapsed time
        if record_count <= 5000:
            create_and_save_model("model_1", "Model 1 using data for the first 5,000 records or 5 minutes")
        elif record_count <= 10000:
            create_and_save_model("model_2", "Model 2 using data for the first 10,000 records or 10 minutes")
        elif record_count <= 15000:
            create_and_save_model("model_3", "Model 3 using data for all records")

    except Exception as e:
        print(f"Error dalam apply_kmeans untuk batch {batch_id}: {e}")

# Trigger the apply_kmeans function on each incoming batch
query = dataset.writeStream \
    .outputMode("append") \
    .foreachBatch(apply_kmeans) \
    .start()

# Wait for termination of the stream
query.awaitTermination()
