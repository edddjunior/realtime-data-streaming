from pyspark.sql import DataFrame, DataFrame, DataFrame, SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from config import configuration

def main():
    spark = SparkSession.builder \
        .appName("DataStreaming") \
        .config(
            "spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,"
            "org.apache.hadoop:hadoop-aws:3.2.0,"
            "com.amazonaws:aws-java-sdk-bundle:1.11.375") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY')) \
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY')) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

    # adjust log level to minimize the console output on executors
    spark.sparkContext.setLogLevel("WARN")

    # vehicle schema
    vehicleSchema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuel_type", StringType(), True)
    ])

    # Schema para GPS Data
    gpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicle_type", StringType(), True)
    ])

    # Schema para Traffic Data
    trafficSchema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("intensity", IntegerType(), True)
    ])

    # Schema para Weather Data
    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("condition", StringType(), True),
        StructField("preciptation", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("air_quality", DoubleType(), True)
    ])

    # Schema para Emergency Incident Data
    emergencySchema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("type", StringType(), True),
        StructField("severity", IntegerType(), True),
        StructField("description", StringType(), True),
        StructField("status", StringType(), True)
    ])

    def read_kafka_topic(topic, schema):
        return spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "broker:29092") \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .load() \
            .selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*") \
            .withWatermark("timestamp", "2 minutes")
    
    def stream_writer(input: DataFrame, checkpoint_folder, output_folder):
        return input.writeStream \
            .format("parquet") \
            .option("checkpointLocation", checkpoint_folder) \
            .option("path", output_folder) \
            .outputMode("append") \
            .start()

    vehicleDF = read_kafka_topic("vehicle_data", vehicleSchema).alias("vehicle")
    gpsDF = read_kafka_topic("gps_data", gpsSchema).alias("gps") 
    trafficDF = read_kafka_topic("traffic_data", trafficSchema).alias("traffic")
    weatherDF = read_kafka_topic("weather_data", weatherSchema).alias("weather")
    emergencyDF = read_kafka_topic("emergency_data", emergencySchema).alias("emergency")

    query1 = stream_writer(
        vehicleDF, 
        "s3a://edddjunior-spark-streaming-data/checkpoints/vehicle_data", 
        "s3a://edddjunior-spark-streaming-data/data/vehicle_data"
    )
    query2 = stream_writer(
        gpsDF, 
        "s3a://edddjunior-spark-streaming-data/checkpoints/gps_data", 
        "s3a://edddjunior-spark-streaming-data/data/gps_data"
    )
    query3 = stream_writer(
        trafficDF, 
        "s3a://edddjunior-spark-streaming-data/checkpoints/traffic_data", 
        "s3a://edddjunior-spark-streaming-data/data/traffic_data"
    )
    query4 = stream_writer(
        weatherDF, 
        "s3a://edddjunior-spark-streaming-data/checkpoints/weather_data", 
        "s3a://edddjunior-spark-streaming-data/data/weather_data"
    )
    query5 = stream_writer(
        emergencyDF, 
        "s3a://edddjunior-spark-streaming-data/checkpoints/emergency_data", 
        "s3a://edddjunior-spark-streaming-data/data/emergency_data"
    )

    query5.awaitTermination()

if __name__ == "__main__":
    main()