package com.fabiogouw.spark.awsmessaging;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class AWSMessagingSinkProvider implements TableProvider, DataSourceRegister {
    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        StructField valueField = new StructField("value", DataTypes.StringType, true, Metadata.empty());
        StructField msgAttributeField = new StructField("msgAttributes",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true, Metadata.empty());
        return new StructType(new StructField[]{ valueField, msgAttributeField });
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return new AWSMessagingSinkTable(schema);
    }

    @Override
    public String shortName() {
        return "awsmessaging";
    }
}
