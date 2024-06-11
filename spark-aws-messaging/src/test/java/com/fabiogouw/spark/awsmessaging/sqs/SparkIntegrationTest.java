package com.fabiogouw.spark.awsmessaging.sqs;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

public abstract class SparkIntegrationTest {

    private static final Network network = Network.newNetwork();
    private static final String libSparkAWSMessaging = "spark-aws-messaging-1.1.1.jar";
    private static final String libAWSJavaSdkCore = "aws-java-sdk-core-1.12.13.jar";
    private static final String libAWSJavaSdkSqs = "aws-java-sdk-sqs-1.12.13.jar";

    @Container
    private final GenericContainer spark;

    @Container
    private final LocalStackContainer localstack;

    public SparkIntegrationTest(String sparkImage) {
        spark = new GenericContainer(DockerImageName.parse(sparkImage))
                .withCopyFileToContainer(MountableFile.forHostPath("build/resources/test/.", 0777), "/home/")
                .withCopyFileToContainer(MountableFile.forHostPath("build/libs/" + libSparkAWSMessaging, 0445), "/home/")
                .withCopyFileToContainer(MountableFile.forHostPath("build/libs/deps/" + libAWSJavaSdkCore, 0445), "/home/")
                .withCopyFileToContainer(MountableFile.forHostPath("build/libs/deps/" + libAWSJavaSdkSqs, 0445), "/home/")
                .withNetwork(network)
                .withEnv("AWS_ACCESS_KEY_ID", "test")
                .withEnv("AWS_SECRET_KEY", "test")
                .withEnv("SPARK_MODE", "master");
        localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:latest"))
                .withNetwork(network)
                .withNetworkAliases("localstack")
                .withEnv("SQS_ENDPOINT_STRATEGY", "off")
                .withServices(SQS);
    }

    private AmazonSQS configureQueue(boolean isFIFO) {
        AmazonSQS sqs = AmazonSQSClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        localstack.getEndpointOverride(SQS).toString(),
                        localstack.getRegion()))
                .withCredentials(new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials(localstack.getAccessKey(), localstack.getSecretKey())))
                .build();
        String queueName = "my-test";
        Map<String, String> queueAttributes = new HashMap<>();
        if(isFIFO) {
            queueName += ".fifo";
            queueAttributes.put("FifoQueue", "true");
            queueAttributes.put("ContentBasedDeduplication", "true");
        }
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName).withAttributes(queueAttributes);
        sqs.createQueue(createQueueRequest);
        return sqs;
    }

    private AmazonSQS configureQueue() {
        return configureQueue(false);
    }

    private ExecResult execSparkJob(String script, String... args) throws IOException, InterruptedException {
        String[] command = ArrayUtils.addAll(new String[] {"spark-submit",
                "--jars",
                "/home/" + libSparkAWSMessaging + ",/home/" + libAWSJavaSdkCore + ",/home/" + libAWSJavaSdkSqs,
                "--master",
                "local",
                script}, args);
        ExecResult result = spark.execInContainer(command);
        System.out.println(result.getStdout());
        System.out.println(result.getStderr());
        return result;
    }

    private String getHostAccessibleQueueUrl(AmazonSQS sqs, String queueName) {
        return sqs.getQueueUrl(queueName).getQueueUrl()
                .replace("localstack", localstack.getHost())
                .replace("4566", localstack.getMappedPort(4566).toString());
    }

    private List<Message> getMessagesPut(AmazonSQS sqs, boolean isFIFO) {
        final String queueName = "my-test" + (isFIFO ? ".fifo": "");
        final String queueUrl = getHostAccessibleQueueUrl(sqs, queueName);
        final ReceiveMessageRequest request = new ReceiveMessageRequest(queueUrl)
                .withMaxNumberOfMessages(10)
                .withAttributeNames("All")
                .withMessageAttributeNames("All");
        final ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(request);
        return receiveMessageResult.getMessages();
    }

    private List<Message> getMessagesPut(AmazonSQS sqs){
        return getMessagesPut(sqs, false);
    }

    @Test
    void when_DataframeContainsValueColumn_should_PutAnSQSMessageUsingSpark() throws IOException, InterruptedException {
        // arrange
        AmazonSQS sqs = configureQueue();
        //Thread.sleep(30000);
        // act
        ExecResult result = execSparkJob("/home/sqs_write.py",
                "/home/sample.txt",
                "http://localstack:4566");
        // assert
        assertThat(result.getExitCode()).as("Spark job should execute with no errors").isZero();
        Message message = getMessagesPut(sqs).get(0);
        assertThat(message.getBody()).isEqualTo("my message body");  // the same value in resources/sample.txt
    }

    @Test
    void when_DataframeContainsValueColumnAndMultipleLines_should_PutAsManySQSMessagesInQueue() throws IOException, InterruptedException {
        // arrange
        AmazonSQS sqs = configureQueue();
        // act
        ExecResult result = execSparkJob("/home/sqs_write.py",
                "/home/multiline_sample.txt",
                "http://localstack:4566");
        // assert
        assertThat(result.getExitCode()).as("Spark job should execute with no errors").isZero();
        List<Message> messages = getMessagesPut(sqs);
        assertThat(messages).size().isEqualTo(10);
    }

    @Test
    void when_DataframeContainsDataExceedsSQSSizeLimit_should_FailWholeBatch() throws IOException, InterruptedException {
        // arrange
        AmazonSQS sqs = configureQueue();
        // act
        ExecResult result = execSparkJob("/home/sqs_write.py",
                "/home/large_sample.txt",
                "http://localstack:4566");
        // assert
        assertThat(result.getExitCode()).as("Spark job should execute fail").isNotZero();
        assertThat(result.getStdout()).as("Spark job should fail due to exceeding size limit").contains("Batch requests cannot be longer than 262144 bytes");
        List<Message> messages = getMessagesPut(sqs);
        assertThat(messages).size().as("No messages should be written when the batch fails").isZero();
    }

    @Test
    void when_DataframeContainsLinesThatExceedsSQSMessageSizeLimit_should_ThrowAnException() throws IOException, InterruptedException {
        // arrange
        AmazonSQS sqs = configureQueue();
        HashMap<String, String> attributes = new HashMap<>();
        attributes.put("MaximumMessageSize", Integer.toString(1024));
        sqs.setQueueAttributes(new SetQueueAttributesRequest(getHostAccessibleQueueUrl(sqs, "my-test"), attributes));
        // act
        ExecResult result = execSparkJob("/home/sqs_write.py",
                "/home/multiline_large_sample.txt",
                "http://localstack:4566");
        // assert
        assertThat(result.getExitCode()).as("Spark job should execute fail").isNotZero();
        assertThat(result.getStdout()).as("Spark job should fail due to exceeding size limit").contains("Some messages failed to be sent to the SQS queue");
        List<Message> messages = getMessagesPut(sqs);
        assertThat(messages).size().as("Only messages up to 1024 should be written").isEqualTo(2);
    }

    @Test
    void when_DataframeContainsGroupIdColumn_should_PutAnSQSMessageWithMessageGroupIdUsingSpark() throws IOException, InterruptedException {
        // arrange
        AmazonSQS sqs = configureQueue(true);
        // act
        ExecResult result = execSparkJob("/home/sqs_write_with_groupid.py",
                "http://localstack:4566");
        // assert
        assertThat(result.getExitCode()).as("Spark job should execute with no errors").isZero();
        Message message = getMessagesPut(sqs, true).get(0);
        assertThat(message.getAttributes()).containsKey("MessageGroupId")
                .containsValue("id1");
    }

    @Test
    void when_DataframeContainsMsgAttributesColumn_should_PutAnSQSMessageWithMessageAttributesUsingSpark() throws IOException, InterruptedException {
        // arrange
        AmazonSQS sqs = configureQueue();
        // act
        ExecResult result = execSparkJob("/home/sqs_write_with_msgattribs.py",
                "http://localstack:4566");
        // assert
        assertThat(result.getExitCode()).as("Spark job should execute with no errors").isZero();
        Message message = getMessagesPut(sqs).get(0);
        assertThat(message.getMessageAttributes().get("attribute-a").getStringValue()).isEqualTo("1000");
        assertThat(message.getMessageAttributes().get("attribute-b").getStringValue()).isEqualTo("2000");
    }
}