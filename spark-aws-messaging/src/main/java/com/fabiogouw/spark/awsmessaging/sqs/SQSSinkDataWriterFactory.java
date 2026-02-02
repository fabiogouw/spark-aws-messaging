package com.fabiogouw.spark.awsmessaging.sqs;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;

public class SQSSinkDataWriterFactory implements DataWriterFactory {

    private final SQSSinkOptions options;
    // optional injected builder (used by tests)
    private final software.amazon.awssdk.services.sqs.SqsClientBuilder injectedBuilder;

    public SQSSinkDataWriterFactory(SQSSinkOptions options) {
        this(options, null);
    }

    // test-friendly constructor that accepts a pre-configured SqsClientBuilder
    public SQSSinkDataWriterFactory(SQSSinkOptions options, software.amazon.awssdk.services.sqs.SqsClientBuilder injectedBuilder) {
        this.options = options;
        this.injectedBuilder = injectedBuilder;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {

        final SqsClient sqs = getAmazonSQS();
        GetQueueUrlRequest.Builder queueUrlRequestBuilder = GetQueueUrlRequest.builder().queueName(options.getQueueName());
        if(!options.getQueueOwnerAWSAccountId().isEmpty()) {
            queueUrlRequestBuilder.queueOwnerAWSAccountId(options.getQueueOwnerAWSAccountId());
        }
        final GetQueueUrlResponse queueUrlResponse = sqs.getQueueUrl(queueUrlRequestBuilder.build());
        final String queueUrl = queueUrlResponse.queueUrl();
        return new SQSSinkDataWriter(partitionId,
                taskId,
                sqs,
                options.getBatchSize(),
                queueUrl,
                options.getValueColumnIndex(),
                options.getMsgAttributesColumnIndex(),
                options.getGroupIdColumnIndex());
    }

    private SqsClient getAmazonSQS() {
        SqsClientBuilder clientBuilder = injectedBuilder != null
                ? injectedBuilder
                : SqsClient.builder().httpClientBuilder(UrlConnectionHttpClient.builder())
                    .credentialsProvider(DefaultCredentialsProvider.create());
        if(!options.getEndpoint().isEmpty()) {
            clientBuilder.endpointOverride(java.net.URI.create(options.getEndpoint()));
        }
        // map region string to Region enum if possible
        if(options.getRegion() != null && !options.getRegion().isEmpty()) {
            clientBuilder.region(Region.of(options.getRegion()));
        }
        return clientBuilder.build();
    }
}
