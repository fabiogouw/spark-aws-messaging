package com.fabiogouw.spark;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;

public class SQSSinkDataWriterFactory implements DataWriterFactory {

    private SQSSinkOptions options;

    public SQSSinkDataWriterFactory(SQSSinkOptions options) {
        this.options = options;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {

        AmazonSQS sqs = AmazonSQSClientBuilder.standard()
                .withRegion(options.getRegion())
                .build();

        return new SQSSinkDataWriter(partitionId, taskId, sqs, options.getBatchSize(), options.getQueueName());
    }
}
