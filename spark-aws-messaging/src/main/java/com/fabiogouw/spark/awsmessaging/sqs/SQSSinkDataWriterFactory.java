package com.fabiogouw.spark.awsmessaging.sqs;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;

public class SQSSinkDataWriterFactory implements DataWriterFactory {

    private SQSSinkOptions options;

    public SQSSinkDataWriterFactory(SQSSinkOptions options) {
        this.options = options;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {

        AmazonSQSClientBuilder clientBuilder = AmazonSQSClientBuilder.standard();
        if(!options.getEndpoint().isEmpty()) {
            AwsClientBuilder.EndpointConfiguration endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(
                    options.getEndpoint(), options.getRegion());
            clientBuilder.withEndpointConfiguration(endpointConfiguration);
        }
        else {
            clientBuilder.withRegion(options.getRegion());
        }
        AmazonSQS sqs = clientBuilder.build();
        return new SQSSinkDataWriter(partitionId,
                taskId,
                sqs,
                options.getBatchSize(),
                options.getQueueName(),
                options.getValueColumnIndex(),
                options.getMsgAttributesColumnIndex());
    }
}
