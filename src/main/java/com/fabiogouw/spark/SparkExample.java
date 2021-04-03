package com.fabiogouw.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SparkExample {
    public static void main(String[] args) {

        if(args.length != 6) {
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
                .option("accessKey", args[1])
                .option("secretKey", args[2])
                .option("region", args[3])
                .option("queueName", args[4])
                .option("batchSize", args[5])
                .save();
        spark.stop();
    }
}
