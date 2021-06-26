package com.fabiogouw.spark.awsmessaging.sqs;

import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.util.Objects;

public class SQSSinkWriterCommitMessage implements WriterCommitMessage {

    private final int partitionId;
    private final long taskId;

    public SQSSinkWriterCommitMessage(int partitionId, long taskId) {
        this.partitionId = partitionId;
        this.taskId = taskId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SQSSinkWriterCommitMessage)) return false;
        SQSSinkWriterCommitMessage that = (SQSSinkWriterCommitMessage) o;
        return partitionId == that.partitionId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionId);
    }
}
