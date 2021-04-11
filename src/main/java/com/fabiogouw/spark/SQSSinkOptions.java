package com.fabiogouw.spark;

import java.io.Serializable;

public class SQSSinkOptions implements Serializable {
    private String region;
    private String queueName;
    private int batchSize;

    public SQSSinkOptions(String region, String queueName, int batchSize) {
        this.region = region;
        this.queueName = queueName;
        this.batchSize = batchSize;
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
}
