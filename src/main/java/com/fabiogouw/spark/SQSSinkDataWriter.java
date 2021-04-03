package com.fabiogouw.spark;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;

public class SQSSinkDataWriter implements DataWriter<InternalRow> {

    private final int partitionId;
    private final long taskId;
    private final AmazonSQS sqs;
    private final List<SendMessageBatchRequestEntry> messages = new ArrayList<SendMessageBatchRequestEntry>();
    private final int batchMaxSize;
    private final String queueUrl;

    public SQSSinkDataWriter(int partitionId, long taskId, AmazonSQS sqs, int batchMaxSize, String queueName) {
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.batchMaxSize = batchMaxSize;
        this.sqs = sqs;
        queueUrl = sqs.getQueueUrl(queueName).getQueueUrl();
    }

    @Override
    public void write(InternalRow record) throws IOException {
        messages.add(new SendMessageBatchRequestEntry()
                .withMessageBody(record.getString(0))
                .withId(UUID.randomUUID().toString())
        );
        if(messages.size() >= batchMaxSize) {
            sendMessages();
        }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        try {
            if(messages.size() > 0) {
                sendMessages();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new SQSSinkWriterCommitMessage(partitionId, taskId);
    }

    @Override
    public void abort() throws IOException {

    }

    @Override
    public void close() throws IOException {

    }

    private void sendMessages() {
        SendMessageBatchRequest batch = new SendMessageBatchRequest()
                .withQueueUrl(queueUrl)
                .withEntries(messages);
        sqs.sendMessageBatch(batch);
        messages.clear();
    }
}
