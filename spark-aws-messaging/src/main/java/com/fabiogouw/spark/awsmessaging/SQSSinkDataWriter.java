package com.fabiogouw.spark.awsmessaging;

import com.amazonaws.services.sqs.model.MessageAttributeValue;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.io.IOException;
import java.util.*;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import org.apache.spark.sql.types.DataTypes;

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
        final MapData map = record.getMap(msgAttribusColumnIndex);
        SendMessageBatchRequestEntry msg = new SendMessageBatchRequestEntry()
                .withMessageBody(record.getString(valueColumnIndex))
                .withMessageAttributes(convertMapData(map))
                .withId(UUID.randomUUID().toString());
        messages.add(msg);
        if(messages.size() >= batchMaxSize) {
            sendMessages();
        }
    }

    private Map<String, MessageAttributeValue> convertMapData(MapData map) {
        final Map<String, MessageAttributeValue> attributes = new HashMap<>();
        if(map.numElements() > 0) {
            ArrayData actualKeys = map.keyArray();
            ArrayData actualValues = map.valueArray();
            for (int i = 0; i < map.numElements(); i++) {
                Object actualKey = actualKeys.get(i, DataTypes.StringType);
                Object actualValue = actualValues.get(i, DataTypes.StringType);
                attributes.put(actualKey.toString(), new MessageAttributeValue()
                        .withDataType("String")
                        .withStringValue(actualValue.toString()));
            }
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
