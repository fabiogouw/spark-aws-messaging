package com.fabiogouw.spark.awsmessaging.sqs;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static com.fabiogouw.spark.awsmessaging.sqs.SQSSinkOptions.Service.SQS;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SQSSinkDataWriterFactoryTest {
    @Test
    public void when_CustomEndpointIsNotProvided_should_CreateDataWriterWithOnlyRegionConfiguration() {
        // Arrange
        AmazonSQS mockSqs = mock(AmazonSQS.class);
        when(mockSqs.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(new GetQueueUrlResult());
        AmazonSQSClientBuilder mockSqsClientBuilder = mock(AmazonSQSClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqs);
        try (MockedStatic<AmazonSQSClientBuilder> utilities = Mockito.mockStatic(AmazonSQSClientBuilder.class)) {
            utilities.when(AmazonSQSClientBuilder::standard).thenReturn(mockSqsClientBuilder);
            SQSSinkDataWriterFactory sut = new SQSSinkDataWriterFactory(new SQSSinkOptions("us-east-1",
                    null,
                    "my-test",
                    null,
                    3,
                    SQS,
                    0,
                    -1,
                    -1));
            // Act
            DataWriter<InternalRow> writer = sut.createWriter(0, 0);
            // Assert
            assertThat(writer).isNotNull();
            verify(mockSqsClientBuilder, times(0)).withEndpointConfiguration(any(AwsClientBuilder.EndpointConfiguration.class));
            verify(mockSqsClientBuilder, times(1)).withRegion(any(String.class));
        }
    }

    @Test
    public void when_CustomEndpointIsProvided_should_CreateDataWriterWithEndpointConfiguration() {
        // Arrange
        AmazonSQS mockSqs = mock(AmazonSQS.class);
        when(mockSqs.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(new GetQueueUrlResult());
        AmazonSQSClientBuilder mockSqsClientBuilder = mock(AmazonSQSClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqs);
        try (MockedStatic<AmazonSQSClientBuilder> utilities = Mockito.mockStatic(AmazonSQSClientBuilder.class)) {
            utilities.when(AmazonSQSClientBuilder::standard).thenReturn(mockSqsClientBuilder);
            SQSSinkDataWriterFactory sut = new SQSSinkDataWriterFactory(new SQSSinkOptions("us-east-1",
                    "http://host:4566",
                    "my-test",
                    null,
                    3,
                    SQS,
                    0,
                    -1,
                    -1));
            // Act
            DataWriter<InternalRow> writer = sut.createWriter(0, 0);
            // Assert
            assertThat(writer).isNotNull();
            verify(mockSqsClientBuilder, times(1)).withEndpointConfiguration(any(AwsClientBuilder.EndpointConfiguration.class));
        }
    }

    @Test
    public void when_AnotherOwnerAWSAccountIdIsProvided_should_ConfigureUrlRequestWithThisQueueOwnerAWSAccountId() {
        // Arrange
        AmazonSQS mockSqs = mock(AmazonSQS.class);
        when(mockSqs.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(new GetQueueUrlResult());
        AmazonSQSClientBuilder mockSqsClientBuilder = mock(AmazonSQSClientBuilder.class);
        when(mockSqsClientBuilder.build()).thenReturn(mockSqs);

        try (MockedStatic<AmazonSQSClientBuilder> utilities = Mockito.mockStatic(AmazonSQSClientBuilder.class)) {
            utilities.when(AmazonSQSClientBuilder::standard).thenReturn(mockSqsClientBuilder);
            try(MockedConstruction<GetQueueUrlRequest> mockGetQueueUrlRequest = Mockito.mockConstruction(GetQueueUrlRequest.class)) {

                SQSSinkDataWriterFactory sut = new SQSSinkDataWriterFactory(new SQSSinkOptions("us-east-1",
                        null,
                        "my-test",
                        "1234567890",
                        3,
                        SQS,
                        0,
                        -1,
                        -1));
                // Act
                DataWriter<InternalRow> writer = sut.createWriter(0, 0);
                // Assert
                assertThat(writer).isNotNull();
                verify(mockGetQueueUrlRequest.constructed().get(0), times(1)).setQueueOwnerAWSAccountId("1234567890");
            }
        }
    }
}
