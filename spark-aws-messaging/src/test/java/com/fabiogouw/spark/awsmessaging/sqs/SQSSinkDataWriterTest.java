package com.fabiogouw.spark.awsmessaging.sqs;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.MockitoJUnitRunner;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
class SQSSinkDataWriterTest {

    private InternalRow createInternalRow(Object... values) {
        Seq<Object> x = JavaConverters.asScalaBuffer(new ArrayList<>(Arrays.asList(values))).toSeq();
        return InternalRow.fromSeq(x);
    }

    @Test
    void when_ProvidedLessRowsThanBatchSize_should_NotSendMessageBatchToAWS() throws IOException {
        // Arrange
        SqsClient mockSqs = mock(SqsClient.class);
        when(mockSqs.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().queueUrl("http://q").build());
        try(SQSSinkDataWriter sut = new SQSSinkDataWriter(1, 2, mockSqs, 3, "", 0, -1, -1)) {
            // Act
            sut.write(createInternalRow(UTF8String.fromString("x")));
        }
        // Assert
        verify(mockSqs, times(0)).sendMessageBatch(any(SendMessageBatchRequest.class));
    }

    @Test
    void when_ProvidedRowsEqualsToBatchSize_should_SendMessageBatchToAWSOneTime() throws IOException {
        // Arrange
        int batchSize = 3;
        SqsClient mockSqs = mock(SqsClient.class);
        when(mockSqs.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().queueUrl("http://q").build());
        when(mockSqs.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(SendMessageBatchResponse.builder().build());
        try(SQSSinkDataWriter sut = new SQSSinkDataWriter(1, 2, mockSqs, batchSize, "", 0, -1, -1)) {
            // Act
            for(int i = 0; i < batchSize; i++) {
                sut.write(createInternalRow(UTF8String.fromString("x")));
            }
        }
        // Assert
        verify(mockSqs, times(1)).sendMessageBatch(any(SendMessageBatchRequest.class));
    }

    @Test
    void when_Committing_should_SendRemainingMessageBatchToAWSOneTime() throws IOException {
        // Arrange
        SqsClient mockSqs = mock(SqsClient.class);
        when(mockSqs.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().queueUrl("http://q").build());
        when(mockSqs.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(SendMessageBatchResponse.builder().build());
        try(SQSSinkDataWriter sut = new SQSSinkDataWriter(1, 2, mockSqs, 3, "", 0, -1, -1)) {
            sut.write(createInternalRow(UTF8String.fromString("x")));
            // Act
            sut.commit();
        }
        // Assert
        verify(mockSqs, times(1)).sendMessageBatch(any(SendMessageBatchRequest.class));
    }

    @Test
    void when_PassingAGroupIdColumn_should_SendMessageBatchToAWSOneTimeWithGroupId() throws IOException {
        // Arrange
        SqsClient mockSqs = mock(SqsClient.class);
        when(mockSqs.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().queueUrl("http://q").build());
        when(mockSqs.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(SendMessageBatchResponse.builder().build());
        try(SQSSinkDataWriter sut = new SQSSinkDataWriter(1, 2, mockSqs, 1, "", 0, -1, 1)) {
            // Act
            sut.write(createInternalRow(UTF8String.fromString("x"), UTF8String.fromString("id")));
        }
        // Assert
        ArgumentCaptor<SendMessageBatchRequest> argumentCaptor = ArgumentCaptor.forClass(SendMessageBatchRequest.class);
        verify(mockSqs).sendMessageBatch(argumentCaptor.capture());
        SendMessageBatchRequest capturedArgument = argumentCaptor.getValue();
        assertThat(capturedArgument.entries().get(0).messageGroupId()).isEqualTo("id");
    }

    @Test
    void when_PassingAMessageAttributeColumn_should_SendMessageBatchToAWSOneTimeWithMessageAttributes() throws IOException {
        // Arrange
        SqsClient mockSqs = mock(SqsClient.class);
        when(mockSqs.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().queueUrl("http://q").build());
        when(mockSqs.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(SendMessageBatchResponse.builder().build());
        ArrayBasedMapData map = new ArrayBasedMapData(new GenericArrayData(Collections.singletonList("attribute-a")), new GenericArrayData(Collections.singletonList("attribute")));
        try(SQSSinkDataWriter sut = new SQSSinkDataWriter(1, 2, mockSqs, 1, "", 0, 1, 0)){
            // Act
            sut.write(createInternalRow(UTF8String.fromString("x"), map));
        }
        // Assert
        ArgumentCaptor<SendMessageBatchRequest> argumentCaptor = ArgumentCaptor.forClass(SendMessageBatchRequest.class);
        verify(mockSqs).sendMessageBatch(argumentCaptor.capture());
        SendMessageBatchRequest capturedArgument = argumentCaptor.getValue();
        assertThat(capturedArgument.entries().get(0).messageAttributes().get("attribute-a").stringValue()).isEqualTo("attribute");
    }
}