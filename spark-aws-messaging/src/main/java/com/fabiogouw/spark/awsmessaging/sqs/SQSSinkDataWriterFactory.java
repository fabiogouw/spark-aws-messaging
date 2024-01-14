package com.fabiogouw.spark.awsmessaging.sqs;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;

public class SQSSinkDataWriterFactory implements DataWriterFactory {

    private final SQSSinkOptions options;

    public SQSSinkDataWriterFactory(SQSSinkOptions options) {
        this.options = options;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {

        final AmazonSQS sqs = getAmazonSQS();
        final GetQueueUrlRequest queueUrlRequest = new GetQueueUrlRequest(options.getQueueName());
        if(!options.getQueueOwnerAWSAccountId().isEmpty()) {
            queueUrlRequest.setQueueOwnerAWSAccountId(options.getQueueOwnerAWSAccountId());
        }
        final String queueUrl = sqs.getQueueUrl(queueUrlRequest).getQueueUrl();
        return new SQSSinkDataWriter(partitionId,
                taskId,
                sqs,
                options.getBatchSize(),
                queueUrl,
                options.getValueColumnIndex(),
                options.getMsgAttributesColumnIndex(),
                options.getGroupIdColumnIndex());
    }

    private AmazonSQS getAmazonSQS() {
        AmazonSQSClientBuilder clientBuilder = AmazonSQSClientBuilder.standard();
        if(!options.getEndpoint().isEmpty()) {
            AwsClientBuilder.EndpointConfiguration endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(
                    options.getEndpoint(), options.getRegion());
            clientBuilder.withEndpointConfiguration(endpointConfiguration);
        }
        else {
            clientBuilder.withRegion(options.getRegion());
        }
        return clientBuilder.build();
    }
}
