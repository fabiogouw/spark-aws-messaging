package com.fabiogouw.spark.awsmessaging.sqs;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DataTypes;

import java.io.IOException;
import java.util.*;

public class SQSSinkDataWriter implements DataWriter<InternalRow> {

    private final int partitionId;
    private final long taskId;
    private final AmazonSQS sqs;
    private final List<SendMessageBatchRequestEntry> messages = new ArrayList<>();
    private final int batchMaxSize;
    private final String queueUrl;
    private final int valueColumnIndex;
    private final int msgAttributesColumnIndex;
    private final int groupIdColumnIndex;

    public SQSSinkDataWriter(int partitionId,
                             long taskId,
                             AmazonSQS sqs,
                             int batchMaxSize,
                             String queueUrl,
                             int valueColumnIndex,
                             int msgAttributesColumnIndex,
                             int groupIdColumnIndex) {
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.batchMaxSize = batchMaxSize;
        this.sqs = sqs;
        this.queueUrl = queueUrl;
        this.valueColumnIndex = valueColumnIndex;
        this.msgAttributesColumnIndex = msgAttributesColumnIndex;
        this.groupIdColumnIndex = groupIdColumnIndex;
    }

    @Override
    public void write(InternalRow record) throws IOException {
        Optional<MapData> msgAttributesData = Optional.empty();
        if(msgAttributesColumnIndex >= 0) {
            msgAttributesData = Optional.of(record.getMap(msgAttributesColumnIndex));
        }
        SendMessageBatchRequestEntry msg = new SendMessageBatchRequestEntry()
                .withMessageBody(record.getString(valueColumnIndex))
                .withMessageAttributes(convertMapData(msgAttributesData))
                .withId(UUID.randomUUID().toString());
        if(groupIdColumnIndex >= 0) {
            msg = msg.withMessageGroupId(record.getString(groupIdColumnIndex));
        }
        messages.add(msg);
        if(messages.size() >= batchMaxSize) {
            sendMessages();
        }
    }

    private Map<String, MessageAttributeValue> convertMapData(Optional<MapData> arrayData) {
        final Map<String, MessageAttributeValue> attributes = new HashMap<>();
        arrayData.ifPresent(mapData -> mapData.foreach(DataTypes.StringType, DataTypes.StringType, (key, value) -> {
            attributes.put(key.toString(), new MessageAttributeValue()
                    .withDataType("String")
                    .withStringValue(value.toString()));
            return null;
        }));
        return attributes;
    }

    @Override
    public WriterCommitMessage commit() {
        if(!messages.isEmpty()) {
            sendMessages();
        }
        return new SQSSinkWriterCommitMessage(partitionId, taskId);
    }

    @Override
    public void abort() {
        // nothing to abort here, since this sink is not atomic
    }

    @Override
    public void close() {
        // nothing to close
    }

    private void sendMessages() {
        final SendMessageBatchRequest batch = new SendMessageBatchRequest()
                .withQueueUrl(queueUrl)
                .withEntries(messages);
        SendMessageBatchResult sendMessageBatchResult = sqs.sendMessageBatch(batch);
        final List<BatchResultErrorEntry> errors = sendMessageBatchResult.getFailed();
        if(!errors.isEmpty()) {
            throw new SQSSinkBatchResultException.Builder().withErrors(errors).build();
        }
        messages.clear();
    }
}
