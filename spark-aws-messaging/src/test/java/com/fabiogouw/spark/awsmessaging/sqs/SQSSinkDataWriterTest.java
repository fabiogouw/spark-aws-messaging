package com.fabiogouw.spark.awsmessaging.sqs;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SQSSinkDataWriterTest {

    private InternalRow createInternalRow(Object... values) {
        Seq<Object> x = JavaConverters.asScalaBuffer(new ArrayList<>(Arrays.asList(values))).toSeq();
        return InternalRow.fromSeq(x);
    }

    @Test
    public void when_ProvidedLessRowsThanBatchSize_should_NotSendMessageBatchToAWS() throws IOException {
        AmazonSQS mockSqs = mock(AmazonSQS.class);
        when(mockSqs.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(new GetQueueUrlResult());
        SQSSinkDataWriter sut = new SQSSinkDataWriter(1, 2, mockSqs, 3, "", "", 0, -1, -1);
        sut.write(createInternalRow(UTF8String.fromString("x")));
        verify(mockSqs, times(0)).sendMessageBatch(any(SendMessageBatchRequest.class));
    }

    @Test
    public void when_ProvidedRowsEqualsToBatchSize_should_SendMessageBatchToAWSOneTime() throws IOException {
        int batchSize = 3;
        AmazonSQS mockSqs = mock(AmazonSQS.class);
        when(mockSqs.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(new GetQueueUrlResult());
        when(mockSqs.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(new SendMessageBatchResult());
        SQSSinkDataWriter sut = new SQSSinkDataWriter(1, 2, mockSqs, batchSize, "", "", 0, -1, -1);
        for(int i = 0; i < batchSize; i++) {
            sut.write(createInternalRow(UTF8String.fromString("x")));
        }
        verify(mockSqs, times(1)).sendMessageBatch(any(SendMessageBatchRequest.class));
    }
}