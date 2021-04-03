package com.fabiogouw.spark;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;

public class SQSSinkWriteBuilder implements WriteBuilder {

    private LogicalWriteInfo info;

    public SQSSinkWriteBuilder(LogicalWriteInfo info) {
        this.info = info;
    }

    @Override
    public BatchWrite buildForBatch() {
        int batchSize = Integer.parseInt(info.options().getOrDefault("batchSize", "1000"));
        SQSSinkOptions options = new SQSSinkOptions(info.options().get("accessKey"),
                info.options().get("secretKey"),
                info.options().get("region"),
                info.options().get("queueName"),
                batchSize);
        return new SQSSinkBatchWrite(options);
    }
}
