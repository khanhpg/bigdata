from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum, avg
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, 
    BooleanType, LongType, TimestampType
)

def create_spark_session():
    """
    Tạo SparkSession với cấu hình cho Kafka và Elasticsearch
    """
    return (SparkSession.builder
            .appName("ZillowKafkaElasticsearchStreaming")
            .master("local[*]")
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,"
                    "org.elasticsearch:elasticsearch-spark-30_2.12:7.17.16")
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
            .config("es.nodes", "localhost:9200")
            .config("es.nodes.wan.only", "true")
            .config("es.index.auto.create", "true")
            .getOrCreate())

def define_input_schema():
    return StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("zpid", StringType(), True),
        StructField("homeStatus", StringType(), True),
        StructField("detailUrl", StringType(), True),
        StructField("address", StringType(), True),
        StructField("streetAddress", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("zipcode", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("homeType", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("zestimate", IntegerType(), True),
        StructField("rentZestimate", IntegerType(), True),
        StructField("taxAssessedValue", IntegerType(), True),
        StructField("lotAreaValue", DoubleType(), True),
        StructField("lotAreaUnit", StringType(), True),
        StructField("bathrooms", IntegerType(), True),
        StructField("bedrooms", IntegerType(), True),
        StructField("livingArea", IntegerType(), True),
        StructField("daysOnZillow", IntegerType(), True),
        StructField("isFeatured", BooleanType(), True),
        StructField("isPreforeclosureAuction", BooleanType(), True),
        StructField("timeOnZillow", IntegerType(), True),
        StructField("isNonOwnerOccupied", BooleanType(), True),
        StructField("isPremierBuilder", BooleanType(), True),
        StructField("isZillowOwned", BooleanType(), True),
        StructField("isShowcaseListing", BooleanType(), True),
        StructField("imgSrc", StringType(), True),
        StructField("hasImage", BooleanType(), True),
        StructField("brokerName", StringType(), True),
        StructField("listingSubType.is_FSBA", BooleanType(), True),
        StructField("priceChange", IntegerType(), True),
        StructField("datePriceChanged", LongType(), True),
        StructField("openHouse", StringType(), True),
        StructField("priceReduction", StringType(), True),
        StructField("unit", StringType(), True),
        StructField("listingSubType.is_openHouse", BooleanType(), True),
        StructField("newConstructionType", StringType(), True),
        StructField("listingSubType.is_newHome", BooleanType(), True),
        StructField("videoCount", IntegerType(), True)
    ])

def setup_kafka_streaming(spark, kafka_brokers, kafka_topic):
    """
    Thiết lập streaming dataframe từ Kafka
    
    :param spark: SparkSession
    :param kafka_brokers: Danh sách Kafka brokers
    :param kafka_topic: Tên topic Kafka
    :return: Streaming DataFrame
    """
    # Đọc dữ liệu từ Kafka
    streaming_df = (spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    
    # Parse giá trị từ Kafka message
    parsed_df = (streaming_df
        .select(
            from_json(
                col("value").cast("string"), 
                define_input_schema()
            ).alias("data")
        )
        .select("data.*")
    )
    
    return parsed_df

def process_streaming_data(streaming_df):
    """
    Xử lý dữ liệu streaming Zillow: 
    - Phân tích giá nhà theo thành phố
    - Thống kê loại nhà
    """
    # Nhóm và phân tích dữ liệu
    # city_property_analysis = (streaming_df
    #     .groupBy("city", "homeType")
    #     .agg(
    #         avg("price").alias("average_price"),
    #         sum("livingArea").alias("total_living_area"),
    #         avg("zestimate").alias("average_zestimate"),
    #         sum("bedrooms").alias("total_bedrooms")
    #     )
    # )
    
    # Tính toán theo cửa sổ thời gian
    windowed_sales = (streaming_df
        .withWatermark("timestamp", "1 minutes")
        .groupBy(
            window("timestamp", "1 minutes"),
            "city"
        )
        .agg(
            avg("price").alias("average_price"),
            sum("livingArea").alias("total_living_area"),
            avg("zestimate").alias("average_zestimate"),
            sum("bedrooms").alias("total_bedrooms")
        )
    )

    return windowed_sales
    # return city_property_analysis

def write_streaming_output(windowed_sales):
    """
    Ghi kết quả streaming ra Elasticsearch và console
    """
    # Xuất ra Elasticsearch
    elasticsearch_query = (windowed_sales
        .writeStream
        .outputMode("append")
        .format("org.elasticsearch.spark.sql")
        .option("es.resource", "example_index")
        .option("checkpointLocation", "/tmp/es_checkpoint")
        .start()
    )
    print("Writing to Elasticsearch...", elasticsearch_query)
    # Xuất ra console (tuỳ chọn, có thể bỏ qua)
    console_query = (windowed_sales
        .writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .start()
    )
    
    return [elasticsearch_query, console_query]

def main():
    # Cấu hình Kafka brokers
    KAFKA_BROKERS = "localhost:9092,localhost:9093,localhost:9094"
    KAFKA_TOPIC = "example_topic"
    
    # Tạo Spark Session
    spark = create_spark_session()
    
    # Giảm mức log
    spark.sparkContext.setLogLevel("WARN")
    
    # Tạo streaming dataframe từ Kafka
    streaming_df = setup_kafka_streaming(spark, KAFKA_BROKERS, KAFKA_TOPIC)
    
    # Xử lý dữ liệu
    processed_data = process_streaming_data(streaming_df)
    
    # Ghi output
    queries = write_streaming_output(processed_data)
    
    # Đợi đến khi streaming kết thúc
    for query in queries:
        query.awaitTermination()

if __name__ == "__main__":
    main()