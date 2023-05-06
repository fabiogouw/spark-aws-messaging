package com.fabiogouw.spark.awsmessaging.sqs;

import com.amazonaws.services.sqs.model.*;
import org.apache.commons.lang3.ArrayUtils;
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
import org.testcontainers.containers.Container.ExecResult;


import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

@Testcontainers
public class SparkIntegrationTest {

    private static Network network = Network.newNetwork();

    @Container
    private final LocalStackContainer localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:0.12.13"))
            .withNetwork(network)
            .withNetworkAliases("localstack")
            .withServices(SQS);

    @Container
    private final GenericContainer spark = new GenericContainer(DockerImageName.parse("bitnami/spark:3.1.2"))
            .withCopyFileToContainer(MountableFile.forHostPath("build/resources/test/.", 0744), "/home/")
            .withCopyFileToContainer(MountableFile.forHostPath("build/libs/.", 0555), "/home/")
            .withNetwork(network)
            .withEnv("AWS_ACCESS_KEY_ID", "test")
            .withEnv("AWS_SECRET_KEY", "test")
            .withEnv("SPARK_MODE", "master");
    
    private AmazonSQS configureQueue() {
        AmazonSQS sqs = AmazonSQSClientBuilder.standard()
                .withEndpointConfiguration(localstack.getEndpointConfiguration(SQS))
                .withCredentials(localstack.getDefaultCredentialsProvider())
                .build();
        sqs.createQueue("my-test");
        return sqs;
    }
    
    private ExecResult execSparkJob(String script, String... args) throws IOException, InterruptedException {
        String[] command = ArrayUtils.addAll(new String[] {"spark-submit", 
                "--jars", 
                "/home/spark-aws-messaging-0.5.0.jar,/home/deps/aws-java-sdk-core-1.12.13.jar,/home/deps/aws-java-sdk-sqs-1.12.13.jar",
                "--master", 
                "local",
                script}, args);
        ExecResult result = spark.execInContainer(command);
        System.out.println(result.getStdout());
        System.out.println(result.getStderr());
        return result;
    }
    
    private String getBodyOfFirstMessagePut(AmazonSQS sqs) {
        String queueUrl = sqs.getQueueUrl("my-test").getQueueUrl()
                .replace("localstack", localstack.getHost());
        List<Message> messages = sqs.receiveMessage(queueUrl)
                .getMessages();
        return messages.get(0).getBody();
    }

    @Test
    public void shouldPutASQSMessageInLocalstackUsingSpark() throws IOException, InterruptedException {
        String expectedBody = "my message body";    // the same value in resources/sample.txt
        
        AmazonSQS sqs = configureQueue();

        ExecResult result = execSparkJob("/home/sqs_write.py",
            "/home/sample.txt",
            "http://localstack:4566");

        assertEquals(0, result.getExitCode());
        assertEquals(expectedBody, getBodyOfFirstMessagePut(sqs));
    }
}
