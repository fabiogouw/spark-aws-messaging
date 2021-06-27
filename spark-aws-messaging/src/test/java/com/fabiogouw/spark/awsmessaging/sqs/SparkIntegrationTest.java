package com.fabiogouw.spark.awsmessaging.sqs;

import com.amazonaws.services.sqs.model.*;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.testcontainers.utility.MountableFile;


import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

@Testcontainers
public class SparkIntegrationTest {

    private static Network network = Network.newNetwork();

    @Container
    public LocalStackContainer localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:0.12.13"))
            .withNetwork(network)
            .withNetworkAliases("localstack")
            .withServices(SQS);

    @Container
    public GenericContainer spark = new GenericContainer(DockerImageName.parse("bitnami/spark:3.1.2"))
            .withCopyFileToContainer(MountableFile.forHostPath("build/resources/test/.", 0744), "/home/")
            .withCopyFileToContainer(MountableFile.forHostPath("build/libs/.", 0555), "/home/")
            .withNetwork(network)
            .withEnv("AWS_ACCESS_KEY_ID", "test")
            .withEnv("AWS_SECRET_KEY", "test")
            .withEnv("SPARK_MODE", "master");

    @Test
    public void shouldPutASQSMessageInLocalstackUsingSpark() throws IOException, InterruptedException {
        String expectedBody = "my message body";    // the same value in resources/sample.txt

        AmazonSQS sqs = AmazonSQSClientBuilder.standard()
                .withEndpointConfiguration(localstack.getEndpointConfiguration(SQS))
                .withCredentials(localstack.getDefaultCredentialsProvider())
                .build();
        sqs.createQueue("my-test");

        org.testcontainers.containers.Container.ExecResult lsResult =
                spark.execInContainer("spark-submit",
                        "--jars", "/home/spark-aws-messaging-0.3.1.jar,/home/deps/aws-java-sdk-core-1.12.12.jar,/home/deps/aws-java-sdk-sqs-1.12.12.jar",
                        "--master", "local",
                        "/home/sqs_write.py",
                        "/home/sample.txt",
                        "http://localstack:4566");

        System.out.println(lsResult.getStdout());
        System.out.println(lsResult.getStderr());

        assertEquals(0, lsResult.getExitCode());

        String queueUrl = sqs.getQueueUrl("my-test").getQueueUrl()
                .replace("localstack", localstack.getContainerIpAddress());
        List<Message> messages = sqs.receiveMessage(queueUrl)
                .getMessages();
        assertEquals(expectedBody, messages.get(0).getBody());
    }
}
