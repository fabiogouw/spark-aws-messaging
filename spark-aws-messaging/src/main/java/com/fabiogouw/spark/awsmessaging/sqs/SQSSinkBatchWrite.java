package com.fabiogouw.spark.awsmessaging.sqs;

import org.apache.spark.sql.connector.write.*;

public class SQSSinkBatchWrite implements BatchWrite {

    private SQSSinkOptions options;

    public SQSSinkBatchWrite(SQSSinkOptions options) {
        this.options = options;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        return new SQSSinkDataWriterFactory(options);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {

    }

    @Override
    public void abort(WriterCommitMessage[] messages) {

    }
}
