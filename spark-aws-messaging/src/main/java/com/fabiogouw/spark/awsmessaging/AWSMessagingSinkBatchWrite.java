package com.fabiogouw.spark.awsmessaging;

import org.apache.spark.sql.connector.write.*;

public class AWSMessagingSinkBatchWrite implements BatchWrite {

    private AWSMessagingSinkOptions options;

    public AWSMessagingSinkBatchWrite(AWSMessagingSinkOptions options) {
        this.options = options;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        return new AWSMessagingSinkDataWriterFactory(options);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {

    }

    @Override
    public void abort(WriterCommitMessage[] messages) {

    }
}
