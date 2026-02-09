package com.fabiogouw.spark.awsmessaging.sqs;

import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

@Testcontainers
class SparkIntegrationTest {

    private static final Network network = Network.newNetwork();
    private static final String LIB_JAR_NAME = "spark-aws-messaging-1.2.0.jar";

    @Container
    private final GenericContainer spark;

    @Container
    private final LocalStackContainer localstack;

    public SparkIntegrationTest() throws IOException {
        var sparkContainer = new GenericContainer(DockerImageName.parse("bitnamilegacy/spark:3.5.1"))
                .withCopyFileToContainer(MountableFile.forHostPath("build/resources/test/.", 0777), "/home/")
                .withCopyFileToContainer(MountableFile.forHostPath("build/libs/" + LIB_JAR_NAME, 0445), "/home/")
                .withNetwork(network)
                .withEnv("AWS_ACCESS_KEY_ID", "test")
                .withEnv("AWS_SECRET_ACCESS_KEY", "test")
                .withEnv("SPARK_MODE", "master");
        spark = copyAllDependencyFilesToContainer(sparkContainer);
        localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:latest"))
                .withNetwork(network)
                .withNetworkAliases("localstack")
                .withEnv("SQS_ENDPOINT_STRATEGY", "off")
                .withServices(SQS);
    }

    private static GenericContainer<?> copyAllDependencyFilesToContainer(GenericContainer<?> container) throws IOException {
        Path dir = Paths.get("build/libs/deps");
        try (Stream<Path> stream = Files.list(dir)) {
            stream.filter(Files::isRegularFile)
                    .forEach(p -> container.withCopyFileToContainer(
                            MountableFile.forHostPath(p.toString(), 0445),"/home/"));
        }
        return container;
    }

    private SqsClient configureQueue(boolean isFIFO) {
        SqsClient sqs = SqsClient.builder()
                .httpClientBuilder(UrlConnectionHttpClient.builder())
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

    private static String[] listFileNames(String dirPath) throws IOException {
        Path dir = Paths.get(dirPath);
        try (Stream<Path> stream = Files.list(dir)) {
            return stream.filter(Files::isRegularFile)
                    .map(p -> p.getFileName().toString())
                    .toArray(String[]::new);
        }
    }

    private static String buildSparkLibPath() throws IOException {
        return "/home/" + LIB_JAR_NAME + ",/home/" + String.join(",/home/", listFileNames("build/libs/deps"));
    }

    private ExecResult execSparkJob(String script, String... args) throws IOException, InterruptedException {
        String[] command = ArrayUtils.addAll(new String[] {"spark-submit",
                "--jars",
                buildSparkLibPath(),
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

    // TODO: this test was being executed twice in the github pipeline. Need to investigate why and fix it.
    //@Test
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