package com.fabiogouw.spark.awsmessaging.sqs;

import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class Spark3_3_2IntegrationTest extends SparkIntegrationTest {

    public Spark3_3_2IntegrationTest() {
        super("bitnami/spark:3.3.2");
    }
}