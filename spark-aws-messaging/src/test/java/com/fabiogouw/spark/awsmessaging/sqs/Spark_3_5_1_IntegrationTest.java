package com.fabiogouw.spark.awsmessaging.sqs;

import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;

@Testcontainers
public class Spark_3_5_1_IntegrationTest extends SparkIntegrationTest {

    public Spark_3_5_1_IntegrationTest() throws IOException {
        super("bitnamilegacy/spark:3.5.1");
    }
}