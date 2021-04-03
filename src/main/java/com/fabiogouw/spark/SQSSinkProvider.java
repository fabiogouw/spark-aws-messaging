package com.fabiogouw.spark;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class SQSSinkProvider implements TableProvider {
    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        StructField structField = new StructField("value", DataTypes.StringType, true, Metadata.empty());
        return new StructType(new StructField[]{ structField });
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return new SQSSinkTable(schema);
    }
}
