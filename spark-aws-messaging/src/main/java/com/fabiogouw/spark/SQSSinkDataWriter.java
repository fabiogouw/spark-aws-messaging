package com.fabiogouw.spark;

import com.amazonaws.services.sqs.model.MessageAttributeValue;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.io.IOException;
import java.util.*;

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
    private final int valueColumnIndex;
    private final int msgAttribusColumnIndex;

    public SQSSinkDataWriter(int partitionId,
                             long taskId,
                             AmazonSQS sqs,
                             int batchMaxSize,
                             String queueName,
                             int valueColumnIndex,
                             int msgAttribusColumnIndex) {
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.batchMaxSize = batchMaxSize;
        this.sqs = sqs;
        queueUrl = sqs.getQueueUrl(queueName).getQueueUrl();
        this.valueColumnIndex = valueColumnIndex;
        this.msgAttribusColumnIndex = msgAttribusColumnIndex;
    }

    @Override
    public void write(InternalRow record) throws IOException {
        SendMessageBatchRequestEntry msg = new SendMessageBatchRequestEntry()
                .withMessageBody(record.getString(valueColumnIndex))
                .withId(UUID.randomUUID().toString());
        if(msgAttribusColumnIndex >= 0) {
            final MapData map = record.getMap(msgAttribusColumnIndex);
            msg.setMessageAttributes(convertMapData(map));
        }
        messages.add(msg);
        if(messages.size() >= batchMaxSize) {
            sendMessages();
        }
    }

    private Map<String, MessageAttributeValue> convertMapData(MapData map) {
        final Map<String, MessageAttributeValue> attributes = new HashMap<>();
        for (int i = 0; i < map.numElements(); i++) {
            String key = map.keyArray().array()[i].toString();
            String value = map.valueArray().array()[i].toString();
            MessageAttributeValue msgAttribute = new MessageAttributeValue();
            msgAttribute.setStringValue(value);
            attributes.put(key, msgAttribute);
        }
        return attributes;
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
        return new AWSMessagingSinkWriterCommitMessage(partitionId, taskId);
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
