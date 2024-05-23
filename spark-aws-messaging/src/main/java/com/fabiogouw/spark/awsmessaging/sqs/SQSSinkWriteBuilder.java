package com.fabiogouw.spark.awsmessaging.sqs;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

public class SQSSinkWriteBuilder implements WriteBuilder {

    private final LogicalWriteInfo info;
    private static final String MESSAGE_ATTRIBUTES_COLUMN_NAME = "msg_attributes";
    private static final String GROUP_ID_COLUMN_NAME = "group_id";
    private static final String VALUE_COLUMN_NAME = "value";

    public SQSSinkWriteBuilder(LogicalWriteInfo info) {
        this.info = info;
    }

    @Override
    public BatchWrite buildForBatch() {
        int batchSize = Integer.parseInt(info.options().getOrDefault("batchSize", "1000"));
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
}
