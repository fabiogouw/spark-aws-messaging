package com.fabiogouw.spark.awsmessaging.sqs;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
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
    private final SqsClient sqs;
    private final List<SendMessageBatchRequestEntry> messages = new ArrayList<>();
    private final int batchMaxSize;
    private final String queueUrl;
    private final int valueColumnIndex;
    private final int msgAttributesColumnIndex;
    private final int groupIdColumnIndex;

    public SQSSinkDataWriter(int partitionId,
                             long taskId,
                             SqsClient sqs,
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
        SendMessageBatchRequestEntry.Builder msgBuilder = SendMessageBatchRequestEntry.builder()
                .messageBody(record.getString(valueColumnIndex))
                .messageAttributes(convertMapData(msgAttributesData))
                .id(UUID.randomUUID().toString());
        if(groupIdColumnIndex >= 0) {
            msgBuilder.messageGroupId(record.getString(groupIdColumnIndex));
        }
        SendMessageBatchRequestEntry msg = msgBuilder.build();
        messages.add(msg);
        if(messages.size() >= batchMaxSize) {
            sendMessages();
        }
    }

    private Map<String, MessageAttributeValue> convertMapData(Optional<MapData> arrayData) {
        final Map<String, MessageAttributeValue> attributes = new HashMap<>();
        arrayData.ifPresent(mapData -> mapData.foreach(DataTypes.StringType, DataTypes.StringType, (key, value) -> {
            attributes.put(key.toString(), MessageAttributeValue.builder()
                    .dataType("String")
                    .stringValue(value.toString())
                    .build());
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
        final SendMessageBatchRequest batch = SendMessageBatchRequest.builder()
                .queueUrl(queueUrl)
                .entries(messages)
                .build();
        SendMessageBatchResponse sendMessageBatchResult = sqs.sendMessageBatch(batch);
        final List<BatchResultErrorEntry> errors = sendMessageBatchResult.failed();
        if(!errors.isEmpty()) {
            throw new SQSSinkBatchResultException.Builder().withErrors(errors).build();
        }
        messages.clear();
    }
}
