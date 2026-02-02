package com.fabiogouw.spark.awsmessaging.sqs;

import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;

@Testcontainers
public class Spark3_5_1IntegrationTest extends SparkIntegrationTest {

    public Spark3_5_1IntegrationTest() throws IOException {
        super("bitnami/spark:3.5.1");
    }
}