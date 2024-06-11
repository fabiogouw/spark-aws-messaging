package com.fabiogouw.spark.awsmessaging.sqs;

import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class Spark3_5_1IntegrationTest extends SparkIntegrationTest {

    public Spark3_5_1IntegrationTest() {
        super("bitnami/spark:3.5.1");
    }
}