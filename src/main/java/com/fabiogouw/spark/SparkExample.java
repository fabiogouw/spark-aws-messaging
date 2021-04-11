package com.fabiogouw.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

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

        df.write()
                .format(SQSSinkProvider.class.getCanonicalName())   // "com.fabiogouw.spark.SQSSinkProvider"
                .mode(SaveMode.Append)
                .option("region", args[1])
                .option("queueName", args[2])
                .option("batchSize", args[3])
                .save();
        spark.stop();
    }
}
