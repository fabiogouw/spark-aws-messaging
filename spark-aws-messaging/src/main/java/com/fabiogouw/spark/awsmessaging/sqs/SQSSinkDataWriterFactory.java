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
    private SqsClientBuilder sqsClientBuilder;

    public SQSSinkDataWriterFactory(SQSSinkOptions options) {
        this.options = options;
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
        sqsClientBuilder = SqsClient.builder().httpClientBuilder(UrlConnectionHttpClient.builder())
                .credentialsProvider(DefaultCredentialsProvider.builder().build());
        if(!options.getEndpoint().isEmpty()) {
            sqsClientBuilder.endpointOverride(java.net.URI.create(options.getEndpoint()));
        }
        // map region string to Region enum if possible
        if(options.getRegion() != null && !options.getRegion().isEmpty()) {
            sqsClientBuilder.region(Region.of(options.getRegion()));
        }
        return sqsClientBuilder.build();
    }
}
