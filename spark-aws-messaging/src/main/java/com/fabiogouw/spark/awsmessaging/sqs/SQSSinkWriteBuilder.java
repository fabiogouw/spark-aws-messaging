package com.fabiogouw.spark.awsmessaging.sqs;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

public class SQSSinkWriteBuilder implements WriteBuilder {

    private final LogicalWriteInfo info;
    private static final String messageAttributesColumnName = "msg_attributes";
    private static final String valueColumnName = "value";

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
                schema.fieldIndex(valueColumnName),
                schema.getFieldIndex(messageAttributesColumnName).isEmpty() ? -1 : schema.fieldIndex(messageAttributesColumnName)
                );
        return new SQSSinkBatchWrite(options);
    }
}
