from pyspark import SparkConf, SparkContext
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

def setup_spark_context():
    conf = SparkConf()
    conf.setAppName('nyctaxi_etl')
    conf.set('spark.executor.memory', '2g')
    conf.set('spark.executor.cores', '1')
    conf.set('spark.driver.memory', '10g')
    conf.set('spark.driver.cores', '5')
    spark_context = SparkContext(conf=conf)
    return spark_context

def read_data(spark_context, file_path):
    spark = SparkSession(spark_context)
    df = spark.read.parquet(file_path)
    return df

def process_data(df):
    # You can include the transformations and calculations from your code here
    # For example, calculating the average ratio of trip cost that is tolls
    df_with_tolls_ratio = df.withColumn("tolls_ratio", F.col("tolls_amount") / F.col("total_amount"))
    average_tolls_ratio = df_with_tolls_ratio.select(F.avg("tolls_ratio")).collect()[0][0]
    
    # Add other transformations based on your tasks

    return df_with_tolls_ratio  # or return the final DataFrame after all your transformations

def save_data(df, output_path):
    df.write.mode('overwrite').parquet(output_path)

def main():
    sc = setup_spark_context()
    input_path = '/data/nyctaxi/set1/*.parquet'
    output_path = '/data/nyctaxi/output/processed_data.parquet'
    
    df = read_data(sc, input_path)
    processed_df = process_data(df)
    save_data(processed_df, output_path)
    
    print("ETL process completed successfully!")

if __name__ == "__main__":
    main()

