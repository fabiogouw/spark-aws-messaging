package com.fabiogouw.spark.example;

import com.fabiogouw.spark.awsmessaging.AWSMessagingSinkProvider;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class SparkExample {
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
        df = df.withColumn("msgAttributes", map(lit("EventType"), lit("dummy")));
        df.show();
        df.printSchema();

        df
                .write()
                .format("awsmessaging")
                .mode(SaveMode.Append)
                .option("region", args[1])
                .option("queueName", args[2])
                .option("batchSize", args[3])
                .save();
        spark.stop();
    }
}
