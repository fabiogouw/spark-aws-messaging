import sys
from operator import add
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, MapType
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Missing parameters")
        sys.exit(-1)
    print("Endpoint: " + sys.argv[1])

    spark = SparkSession \
        .builder \
        .appName("SQS Write") \
        .getOrCreate()

    data = [("value 1",{"attribute-a" : 1000, "attribute-b" : "2000"})]

    schema = StructType([
        StructField("value",StringType(),False),
        StructField("msg_attributes",MapType(StringType(), StringType(), True), False)
        ])

    df = spark.createDataFrame(data=data,schema=schema)
    df.show()
    df.printSchema()

    df.write.format("sqs").mode("append") \
        .option("queueName", "my-test") \
        .option("batchSize", "10") \
        .option("endpoint", sys.argv[1]) \
        .save()

    spark.stop()