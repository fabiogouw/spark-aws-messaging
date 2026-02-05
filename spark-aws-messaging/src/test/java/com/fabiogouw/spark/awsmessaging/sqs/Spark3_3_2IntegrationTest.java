package com.fabiogouw.spark.awsmessaging.sqs;

import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;

@Testcontainers
public class Spark3_3_2IntegrationTest extends SparkIntegrationTest {

    public Spark3_3_2IntegrationTest() throws IOException {
        super("bitnamilegacy/spark:3.3.2");
    }
}