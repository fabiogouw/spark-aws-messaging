package com.fabiogouw.spark;

import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.util.Objects;

public class AWSMessagingSinkWriterCommitMessage implements WriterCommitMessage {

    private final int partitionId;
    private final long taskId;

    public AWSMessagingSinkWriterCommitMessage(int partitionId, long taskId) {
        this.partitionId = partitionId;
        this.taskId = taskId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AWSMessagingSinkWriterCommitMessage)) return false;
        AWSMessagingSinkWriterCommitMessage that = (AWSMessagingSinkWriterCommitMessage) o;
        return partitionId == that.partitionId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionId);
    }
}
