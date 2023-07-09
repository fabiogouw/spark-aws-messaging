package com.fabiogouw.spark.awsmessaging.sqs;

import com.amazonaws.services.sqs.model.BatchResultErrorEntry;

import java.util.List;

public class SQSSinkBatchResultException extends RuntimeException {

    public SQSSinkBatchResultException(String message) {
        super(message);
    }

    public SQSSinkBatchResultException(String message, Throwable cause) {
        super(message, cause);
    }

    public static class Builder {
        private List<BatchResultErrorEntry> errors;
        public Builder withErrors(List<BatchResultErrorEntry> errors) {
            this.errors = errors;
            return this;
        }
        public SQSSinkBatchResultException build() {
            String[] failedMessages = errors.stream()
                    .map(BatchResultErrorEntry::getMessage)
                    .distinct()
                    .toArray(String[]::new);
            return new SQSSinkBatchResultException("Some messages failed to be sent to the SQS queue with the following errors: [" + String.join("; ", failedMessages) + "]");
        }
    }
}
