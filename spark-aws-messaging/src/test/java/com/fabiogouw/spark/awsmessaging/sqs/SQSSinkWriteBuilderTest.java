package com.fabiogouw.spark.awsmessaging.sqs;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SQSSinkWriteBuilderTest {

    private final StructType schema = new StructType(new StructField[] {
            new StructField("value", DataTypes.StringType, false, Metadata.empty())
    });

    private LogicalWriteInfo createWriteInfo(Map<String, String> optionsMap) {
        LogicalWriteInfo info = mock(LogicalWriteInfo.class);
        when(info.schema()).thenReturn(schema);
        when(info.options()).thenReturn(new CaseInsensitiveStringMap(optionsMap));
        return info;
    }

    private SQSSinkOptions extractOptions(BatchWrite batchWrite) throws NoSuchFieldException, IllegalAccessException {
        Field optionsField = SQSSinkBatchWrite.class.getDeclaredField("options");
        optionsField.setAccessible(true);
        return (SQSSinkOptions) optionsField.get(batchWrite);
    }

    @Test
    void when_BatchSizeIsNotProvided_should_DefaultToTen() throws NoSuchFieldException, IllegalAccessException {
        Map<String, String> optionsMap = new HashMap<>();
        optionsMap.put("queueName", "my-queue");
        LogicalWriteInfo info = createWriteInfo(optionsMap);

        SQSSinkWriteBuilder sut = new SQSSinkWriteBuilder(info);
        BatchWrite batchWrite = sut.buildForBatch();
        SQSSinkOptions options = extractOptions(batchWrite);

        assertThat(options.getBatchSize()).isEqualTo(10);
    }

    @Test
    void when_BatchSizeIsLessThanOne_should_ThrowIllegalArgumentException() {
        Map<String, String> optionsMap = new HashMap<>();
        optionsMap.put("queueName", "my-queue");
        optionsMap.put("batchSize", "0");
        LogicalWriteInfo info = createWriteInfo(optionsMap);

        SQSSinkWriteBuilder sut = new SQSSinkWriteBuilder(info);

        assertThatThrownBy(sut::buildForBatch)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("batchSize must be between 1 and 10");
    }

    @Test
    void when_BatchSizeIsGreaterThanTen_should_ThrowIllegalArgumentException() {
        Map<String, String> optionsMap = new HashMap<>();
        optionsMap.put("queueName", "my-queue");
        optionsMap.put("batchSize", "11");
        LogicalWriteInfo info = createWriteInfo(optionsMap);

        SQSSinkWriteBuilder sut = new SQSSinkWriteBuilder(info);

        assertThatThrownBy(sut::buildForBatch)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("batchSize must be between 1 and 10");
    }

    @Test
    void when_BatchSizeIsNotAnInteger_should_ThrowIllegalArgumentException() {
        Map<String, String> optionsMap = new HashMap<>();
        optionsMap.put("queueName", "my-queue");
        optionsMap.put("batchSize", "abc");
        LogicalWriteInfo info = createWriteInfo(optionsMap);

        SQSSinkWriteBuilder sut = new SQSSinkWriteBuilder(info);

        assertThatThrownBy(sut::buildForBatch)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("batchSize must be an integer between 1 and 10");
    }
}
