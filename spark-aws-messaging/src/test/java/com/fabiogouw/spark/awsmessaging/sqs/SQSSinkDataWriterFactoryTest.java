package com.fabiogouw.spark.awsmessaging.sqs;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static com.fabiogouw.spark.awsmessaging.sqs.SQSSinkOptions.Service.SQS;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
class SQSSinkDataWriterFactoryTest {
    @Test
    void when_CustomEndpointIsNotProvided_should_CreateDataWriterWithOnlyRegionConfiguration() {
        // Arrange
        SqsClient mockSqs = mock(SqsClient.class);
        when(mockSqs.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().queueUrl("http://q").build());
        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqs);
        SQSSinkDataWriterFactory sut = new SQSSinkDataWriterFactory(new SQSSinkOptions("us-east-1",
                null,
                "my-test",
                null,
                3,
                SQS,
                0,
                -1,
                -1),
                mockSqsClientBuilder);
        // Act
        DataWriter<InternalRow> writer = sut.createWriter(0, 0);
        // Assert
        assertThat(writer).isNotNull();
        verify(mockSqsClientBuilder, times(0)).endpointOverride(any(java.net.URI.class));
        verify(mockSqsClientBuilder, times(1)).region(any());
    }

    @Test
    void when_CustomEndpointIsProvided_should_CreateDataWriterWithEndpointConfiguration() {
        // Arrange
        SqsClient mockSqs = mock(SqsClient.class);
        when(mockSqs.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().queueUrl("http://q").build());
        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqs);
        SQSSinkDataWriterFactory sut = new SQSSinkDataWriterFactory(new SQSSinkOptions("us-east-1",
                "http://host:4566",
                "my-test",
                null,
                3,
                SQS,
                0,
                -1,
                -1),
                mockSqsClientBuilder);
        // Act
        DataWriter<InternalRow> writer = sut.createWriter(0, 0);
        // Assert
        assertThat(writer).isNotNull();
        verify(mockSqsClientBuilder, times(1)).endpointOverride(any(java.net.URI.class));
    }

    @Test
    void when_AnotherOwnerAWSAccountIdIsProvided_should_ConfigureUrlRequestWithThisQueueOwnerAWSAccountId() {
        // Arrange
        SqsClient mockSqs = mock(SqsClient.class);
        when(mockSqs.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().queueUrl("http://q").build());
        SqsClientBuilder mockSqsClientBuilder = mock(SqsClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqs);

        SQSSinkDataWriterFactory sut = new SQSSinkDataWriterFactory(new SQSSinkOptions("us-east-1",
                null,
                "my-test",
                "1234567890",
                3,
                SQS,
                0,
                -1,
                -1),
                mockSqsClientBuilder);
        // Act
        DataWriter<InternalRow> writer = sut.createWriter(0, 0);
        // Assert
        assertThat(writer).isNotNull();
        // capture the request sent to getQueueUrl and assert owner id
        ArgumentCaptor<GetQueueUrlRequest> captor = ArgumentCaptor.forClass(GetQueueUrlRequest.class);
        verify(mockSqs, times(1)).getQueueUrl(captor.capture());
        assertThat(captor.getValue().queueOwnerAWSAccountId()).isEqualTo("1234567890");
    }
}
