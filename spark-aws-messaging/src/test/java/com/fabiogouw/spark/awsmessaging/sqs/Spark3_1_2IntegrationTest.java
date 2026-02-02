package com.fabiogouw.spark.awsmessaging.sqs;

import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;

@Testcontainers
public class Spark3_1_2IntegrationTest extends SparkIntegrationTest {

    public Spark3_1_2IntegrationTest() throws IOException {
        super("bitnami/spark:latest");
    }
}