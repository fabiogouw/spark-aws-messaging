package com.fabiogouw.spark;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import scala.Option;

public class AWSMessagingSinkWriteBuilder implements WriteBuilder {

    private LogicalWriteInfo info;

    public AWSMessagingSinkWriteBuilder(LogicalWriteInfo info) {
        this.info = info;
    }

    @Override
    public BatchWrite buildForBatch() {
        int batchSize = Integer.parseInt(info.options().getOrDefault("batchSize", "1000"));
        final StructType schema = info.schema();
        AWSMessagingSinkOptions.Service service = AWSMessagingSinkOptions.Service.valueOf(
                info.options().getOrDefault("service", "SQS")
                .toUpperCase().trim());
        AWSMessagingSinkOptions options = new AWSMessagingSinkOptions(
                info.options().get("region"),
                info.options().get("queueName"),
                batchSize,
                service,
                schema.fieldIndex("value"),
                schema.getFieldIndex("msgAttributes").isEmpty() ? -1 : schema.fieldIndex("value")
                );
        return new AWSMessagingSinkBatchWrite(options);
    }
}
