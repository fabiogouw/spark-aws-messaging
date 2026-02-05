package com.fabiogouw.spark.awsmessaging.sqs;

import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;

@Testcontainers
public class Spark3_1_2IntegrationTest extends SparkIntegrationTest {

    public Spark3_1_2IntegrationTest() throws IOException {
        super("bitnamilegacy/spark:3.1.2");
    }
}