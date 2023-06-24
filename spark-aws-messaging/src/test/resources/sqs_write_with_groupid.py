import sys
from operator import add
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
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

    data = [("value 1","id1"),
            ("value 2","id2"),
            ("value 3","id1"),
            ("value 4","id2")]

    schema = StructType([
        StructField("value",StringType(),False),
        StructField("group_id",StringType(),False),
    ])

    df = spark.createDataFrame(data=data,schema=schema)
    df.show()
    df.printSchema()

    df.write.format("sqs").mode("append") \
        .option("queueName", "my-test.fifo") \
        .option("batchSize", "10") \
        .option("endpoint", sys.argv[1]) \
        .save()

    spark.stop()