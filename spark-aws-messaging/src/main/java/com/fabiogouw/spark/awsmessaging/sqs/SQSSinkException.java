package com.fabiogouw.spark.awsmessaging.sqs;

public class SQSSinkException extends RuntimeException {

    public SQSSinkException(String message) {
        super(message);
    }

    public SQSSinkException(String message, Throwable cause) {
        super(message, cause);
    }
}
