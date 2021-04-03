package com.fabiogouw.spark;

import java.io.Serializable;

public class SQSSinkOptions implements Serializable {
    private String accessKey;
    private String secretKey;
    private String region;
    private String queueName;
    private int batchSize;

    public SQSSinkOptions(String accessKey, String secretKey, String region, String queueName, int batchSize) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.region = region;
        this.queueName = queueName;
        this.batchSize = batchSize;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
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
