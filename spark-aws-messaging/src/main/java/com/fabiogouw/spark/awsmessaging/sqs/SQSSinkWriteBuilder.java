package com.fabiogouw.spark.awsmessaging.sqs;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

public class SQSSinkWriteBuilder implements WriteBuilder {

    private final LogicalWriteInfo info;
    private static final int MIN_SQS_BATCH_SIZE = 1;
    private static final int MAX_SQS_BATCH_SIZE = 10;
    private static final int DEFAULT_BATCH_SIZE = 10;
    private static final String MESSAGE_ATTRIBUTES_COLUMN_NAME = "msg_attributes";
    private static final String GROUP_ID_COLUMN_NAME = "group_id";
    private static final String VALUE_COLUMN_NAME = "value";

    public SQSSinkWriteBuilder(LogicalWriteInfo info) {
        this.info = info;
    }

    @Override
    public BatchWrite buildForBatch() {
        int batchSize = parseBatchSize(info.options().getOrDefault("batchSize", Integer.toString(DEFAULT_BATCH_SIZE)));
        final StructType schema = info.schema();
        SQSSinkOptions.Service service = SQSSinkOptions.Service.valueOf(
                info.options().getOrDefault("service", "SQS")
                .toUpperCase().trim());
        SQSSinkOptions options = new SQSSinkOptions(
                info.options().get("region"),
                info.options().get("endpoint"),
                info.options().get("queueName"),
                info.options().get("queueOwnerAWSAccountId"),
                batchSize,
                service,
                schema.fieldIndex(VALUE_COLUMN_NAME),
                schema.getFieldIndex(MESSAGE_ATTRIBUTES_COLUMN_NAME).isEmpty() ? -1 : schema.fieldIndex(MESSAGE_ATTRIBUTES_COLUMN_NAME),
                schema.getFieldIndex(GROUP_ID_COLUMN_NAME).isEmpty() ? -1 : schema.fieldIndex(GROUP_ID_COLUMN_NAME)
                );
        return new SQSSinkBatchWrite(options);
    }

    private int parseBatchSize(String rawBatchSize) {
        final int batchSize;
        try {
            batchSize = Integer.parseInt(rawBatchSize);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("batchSize must be an integer between 1 and 10", ex);
        }
        if(batchSize < MIN_SQS_BATCH_SIZE || batchSize > MAX_SQS_BATCH_SIZE) {
            throw new IllegalArgumentException("batchSize must be between 1 and 10");
        }
        return batchSize;
    }
}
