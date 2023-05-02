package com.fabiogouw.spark.awsmessaging.sqs;

import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
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
    private final String queueOwnerAWSAccountId;
    private final int valueColumnIndex;
    private final int msgAttributesColumnIndex;
    private final int groupIdColumnIndex;

    public SQSSinkDataWriter(int partitionId,
                             long taskId,
                             AmazonSQS sqs,
                             int batchMaxSize,
                             String queueName,
                             String queueOwnerAWSAccountId,
                             int valueColumnIndex,
                             int msgAttributesColumnIndex,
                             int groupIdColumnIndex) {
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.batchMaxSize = batchMaxSize;
        this.queueOwnerAWSAccountId = queueOwnerAWSAccountId;
        this.sqs = sqs;
        GetQueueUrlRequest queueUrlRequest = new GetQueueUrlRequest(queueName);
        if(!queueOwnerAWSAccountId.isEmpty()) {
            queueUrlRequest.setQueueOwnerAWSAccountId(queueOwnerAWSAccountId);
        }
        queueUrl = sqs.getQueueUrl(queueUrlRequest).getQueueUrl();
        this.valueColumnIndex = valueColumnIndex;
        this.msgAttributesColumnIndex = msgAttributesColumnIndex;
        this.groupIdColumnIndex = groupIdColumnIndex;
    }

    @Override
    public void write(InternalRow record) throws IOException {
        Optional<ArrayData> arrayData = Optional.empty();
        if(msgAttributesColumnIndex > 0) {
            arrayData = Optional.of(record.getArray(msgAttributesColumnIndex));
        }
        String groupId = groupIdColumnIndex > 0 ? record.getString(groupIdColumnIndex) : null;
        SendMessageBatchRequestEntry msg = new SendMessageBatchRequestEntry()
                .withMessageBody(record.getString(valueColumnIndex))
                .withMessageGroupId(groupId)
                .withMessageAttributes(convertMapData(arrayData))
                .withId(UUID.randomUUID().toString());
        messages.add(msg);
        if(messages.size() >= batchMaxSize) {
            sendMessages();
        }
    }

    private Map<String, MessageAttributeValue> convertMapData(Optional<ArrayData> arrayData) {
        final Map<String, MessageAttributeValue> attributes = new HashMap<>();
        if(arrayData.isPresent()) {
            ArrayData currentArray = arrayData.get();
            for (int i = 0; i < currentArray.numElements(); i++) {
                MapData mapData = currentArray.getMap(i);
                mapData.foreach(DataTypes.StringType, DataTypes.StringType, (key, value) -> {
                    attributes.put(key.toString(), new MessageAttributeValue()
                            .withDataType("String")
                            .withStringValue(value.toString()));
                    return null;
                });
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
