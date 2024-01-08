package com.fabiogouw.spark.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.UUID;

import static org.apache.spark.sql.functions.*;

public class SparkExample {
    /**
     * spark-submit --packages com.amazonaws:aws-java-sdk:1.12.13 \
     * --jars build/libs/spark-aws-messaging-0.3.1.jar \
     * --master local \
     * --class com.fabiogouw.spark.example.SparkExample \
     * build/libs/spark-aws-messaging-example-0.3.0.jar \
     * build/resources/main/sample.txt us-east-2 test 10
     */

    private static final String EVENT_ID_COL = "eventId";
    private static final String EVENT_TYPE_COL = "eventType";

    public static void main(String[] args) {

        if(args.length != 4) {
            throw new IllegalArgumentException("Missing parameters");
        }

        SparkSession spark = SparkSession
            .builder()
            .appName("AWS SQS Sample")
            .getOrCreate();

        Dataset<Row> df = spark
                .read()
                .text(args[0]);
        df = df.withColumn(EVENT_TYPE_COL,  lit("dummy"))
                .withColumn(EVENT_ID_COL,  lit(UUID.randomUUID().toString()));
        df = df.withColumn("msg_attributes",  array(map(lit(EVENT_TYPE_COL), col(EVENT_TYPE_COL)),
                map(lit(EVENT_ID_COL), col(EVENT_ID_COL))));
        df.show();
        df.printSchema();

        df
                .write()
                .format("sqs")
                .mode(SaveMode.Append)
                .option("region", args[1])
                .option("queueName", args[2])
                .option("batchSize", args[3])
                .save();
        spark.stop();
    }
}
