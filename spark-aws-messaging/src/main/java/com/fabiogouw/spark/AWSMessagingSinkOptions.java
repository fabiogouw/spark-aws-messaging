package com.fabiogouw.spark;

import java.io.Serializable;

public class AWSMessagingSinkOptions implements Serializable {
    public enum Service {
        SQS,
        SNS
    }

    private final String region;
    private final String queueName;
    private final int batchSize;
    private final Service service;
    private final int valueColumnIndex;
    private final int msgAttribusColumnIndex;

    public AWSMessagingSinkOptions(String region,
                                   String queueName,
                                   int batchSize,
                                   Service service,
                                   int valueColumnIndex,
                                   int msgAttribusColumnIndex) {
        this.region = region;
        this.queueName = queueName;
        this.batchSize = batchSize;
        this.service = service;
        this.valueColumnIndex = valueColumnIndex;
        this.msgAttribusColumnIndex = msgAttribusColumnIndex;
    }

    public String getRegion() {
        return region;
    }

    public String getQueueName() {
        return queueName;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public Service getService() {
        return service;
    }

    public int getValueColumnIndex() {
        return valueColumnIndex;
    }

    public int getMsgAttribusColumnIndex() {
        return msgAttribusColumnIndex;
    }
}
