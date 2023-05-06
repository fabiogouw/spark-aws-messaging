package com.fabiogouw.spark.awsmessaging.sqs;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

@Testcontainers
public class SparkIntegrationTest {

    private static final Network network = Network.newNetwork();

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
    
    public AmazonSQS configureQueue() {
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
                "/home/spark-aws-messaging-1.0.0.jar,/home/deps/aws-java-sdk-core-1.12.13.jar,/home/deps/aws-java-sdk-sqs-1.12.13.jar",
                "--master", 
                "local",
                script}, args);
        ExecResult result = spark.execInContainer(command);
        System.out.println(result.getStdout());
        System.out.println(result.getStderr());
        return result;
    }
    
    private Message getMessagePut(AmazonSQS sqs) {
        final String queueUrl = sqs.getQueueUrl("my-test").getQueueUrl()
                .replace("localstack", localstack.getHost());
        final ReceiveMessageRequest request = new ReceiveMessageRequest(queueUrl)
                .withAttributeNames("All")
                .withMessageAttributeNames("All");
        final ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(request);
        final List<Message> messages = receiveMessageResult.getMessages();
        return messages.get(0);
    }

    @Test
    public void when_DataframeContainsValueColumn_should_PutAnSQSMessageUsingSpark() throws IOException, InterruptedException {
        // arrange
        String expectedBody = "my message body";    // the same value in resources/sample.txt
        AmazonSQS sqs = configureQueue();
        // act
        ExecResult result = execSparkJob("/home/sqs_write.py",
            "/home/sample.txt",
            "http://localstack:4566");
        // assert
        assertEquals(0, result.getExitCode());
        assertEquals(expectedBody, getMessagePut(sqs).getBody());
    }

    @Test
    public void when_DataframeContainsGroupIdColumn_should_PutAnSQSMessageWithMessageGroupIdUsingSpark() throws IOException, InterruptedException {
        // arrange
        AmazonSQS sqs = configureQueue();
        // act
        ExecResult result = execSparkJob("/home/sqs_write_with_groupid.py",
                "http://localstack:4566");
        // assert
        assertEquals(0, result.getExitCode());
        assertEquals("id 1", getMessagePut(sqs).getAttributes().get("MessageGroupId"));
    }

    @Test
    public void when_DataframeContainsMsgAttributesColumn_should_PutAnSQSMessageWithMessageAttributesUsingSpark() throws IOException, InterruptedException {
        // arrange
        AmazonSQS sqs = configureQueue();
        // act
        ExecResult result = execSparkJob("/home/sqs_write_with_msgattribs.py",
                "http://localstack:4566");
        Message message = getMessagePut(sqs);
        // assert
        assertEquals(0, result.getExitCode());
        assertEquals("1000", message.getMessageAttributes().get("attribute-a").getStringValue());
        assertEquals("2000", message.getMessageAttributes().get("attribute-b").getStringValue());
    }
}
