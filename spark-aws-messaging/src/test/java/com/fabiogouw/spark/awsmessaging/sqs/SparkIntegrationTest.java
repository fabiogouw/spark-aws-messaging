package com.fabiogouw.spark.awsmessaging.sqs;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
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
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

public abstract class SparkIntegrationTest {

    private static final Network network = Network.newNetwork();
    private static final String libSparkAWSMessaging = "spark-aws-messaging-1.1.1.jar";
    private static final String libAWSJavaSdkCore = "software.amazon.awssdk-bom-2.20.0.jar";
    private static final String libAWSJavaSdkSqs = "software.amazon.awssdk-sqs-2.20.0.jar";

    @Container
    private final GenericContainer spark;

    @Container
    private final LocalStackContainer localstack;

    public SparkIntegrationTest(String sparkImage) {
        spark = new GenericContainer(DockerImageName.parse(sparkImage))
                .withCopyFileToContainer(MountableFile.forHostPath("build/resources/test/.", 0777), "/tmp/")
                .withCopyFileToContainer(MountableFile.forHostPath("build/libs/" + libSparkAWSMessaging, 0445), "/tmp/")
                // copy the SDK v2 jars (we assume BOM & module jars are available under build/libs/deps)
                .withCopyFileToContainer(MountableFile.forHostPath("build/libs/deps/software.amazon.awssdk-sqs-2.20.0.jar", 0445), "/tmp/")
                .withCopyFileToContainer(MountableFile.forHostPath("build/libs/deps/software.amazon.awssdk-core-2.20.0.jar", 0445), "/tmp/")
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

    private SqsClient configureQueue(boolean isFIFO) {
        SqsClient sqs = SqsClient.builder()
                .endpointOverride(localstack.getEndpointOverride(SQS))
                .region(Region.of(localstack.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
                .build();
        String queueName = "my-test";
        Map<QueueAttributeName, String> queueAttributes = new HashMap<>();
        if(isFIFO) {
            queueName += ".fifo";
            queueAttributes.put(QueueAttributeName.FIFO_QUEUE, "true");
            queueAttributes.put(QueueAttributeName.CONTENT_BASED_DEDUPLICATION, "true");
        }
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder().queueName(queueName).attributes(queueAttributes).build();
        sqs.createQueue(createQueueRequest);
        return sqs;
    }

    private SqsClient configureQueue() {
        return configureQueue(false);
    }

    private ExecResult execSparkJob(String script, String... args) throws IOException, InterruptedException {
        ExecResult result1 = spark.execInContainer("mkdir /tmp/libs");
        System.out.println(result1.getStdout());
        ExecResult result2 = spark.execInContainer("pwd");
        System.out.println(result2.getStdout());
        String[] command = ArrayUtils.addAll(new String[] {"spark-submit",
                "--jars",
                "/tmp/" + libSparkAWSMessaging + ",/tmp/" + libAWSJavaSdkCore + ",/tmp/" + libAWSJavaSdkSqs,
                "--master",
                "local",
                script}, args);
        ExecResult result = spark.execInContainer(command);
        System.out.println(result.getStdout());
        System.out.println(result.getStderr());
        return result;
    }

    private String getHostAccessibleQueueUrl(SqsClient sqs, String queueName) {
        String url = sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()).queueUrl();
        return url.replace("localstack", localstack.getHost())
                .replace("4566", localstack.getMappedPort(4566).toString());
    }

    private List<Message> getMessagesPut(SqsClient sqs, boolean isFIFO) {
        final String queueName = "my-test" + (isFIFO ? ".fifo": "");
        final String queueUrl = getHostAccessibleQueueUrl(sqs, queueName);
        final ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(10)
                .attributeNamesWithStrings("All")
                .messageAttributeNames("All")
                .build();
        final ReceiveMessageResponse receiveMessageResult = sqs.receiveMessage(request);
        return receiveMessageResult.messages();
    }

    private List<Message> getMessagesPut(SqsClient sqs){
        return getMessagesPut(sqs, false);
    }

    @Test
    void when_DataframeContainsValueColumn_should_PutAnSQSMessageUsingSpark() throws IOException, InterruptedException {
        // arrange
        SqsClient sqs = configureQueue();
        //Thread.sleep(30000);
        // act
        ExecResult result = execSparkJob("/home/sqs_write.py",
                "/home/sample.txt",
                "http://localstack:4566");
        // assert
        assertThat(result.getExitCode()).as("Spark job should execute with no errors").isZero();
        Message message = getMessagesPut(sqs).get(0);
        assertThat(message.body()).isEqualTo("my message body");  // the same value in resources/sample.txt
    }

    @Test
    void when_DataframeContainsValueColumnAndMultipleLines_should_PutAsManySQSMessagesInQueue() throws IOException, InterruptedException {
        // arrange
        SqsClient sqs = configureQueue();
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
        SqsClient sqs = configureQueue();
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
        SqsClient sqs = configureQueue();
        Map<QueueAttributeName, String> attributes = new HashMap<>();
        attributes.put(QueueAttributeName.MAXIMUM_MESSAGE_SIZE, Integer.toString(1024));
        sqs.setQueueAttributes(SetQueueAttributesRequest.builder().queueUrl(getHostAccessibleQueueUrl(sqs, "my-test")).attributes(attributes).build());
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
        SqsClient sqs = configureQueue(true);
        // act
        ExecResult result = execSparkJob("/home/sqs_write_with_groupid.py",
                "http://localstack:4566");
        // assert
        assertThat(result.getExitCode()).as("Spark job should execute with no errors").isZero();
        Message message = getMessagesPut(sqs, true).get(0);
        assertThat(message.attributes().values()).contains("id1");
    }

    @Test
    void when_DataframeContainsMsgAttributesColumn_should_PutAnSQSMessageWithMessageAttributesUsingSpark() throws IOException, InterruptedException {
        // arrange
        SqsClient sqs = configureQueue();
        // act
        ExecResult result = execSparkJob("/home/sqs_write_with_msgattribs.py",
                "http://localstack:4566");
        // assert
        assertThat(result.getExitCode()).as("Spark job should execute with no errors").isZero();
        Message message = getMessagesPut(sqs).get(0);
        assertThat(message.messageAttributes().get("attribute-a").stringValue()).isEqualTo("1000");
        assertThat(message.messageAttributes().get("attribute-b").stringValue()).isEqualTo("2000");
    }
}