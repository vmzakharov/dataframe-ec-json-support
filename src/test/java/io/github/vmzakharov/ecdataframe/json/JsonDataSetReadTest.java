package io.github.vmzakharov.ecdataframe.json;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataset.CsvSchema;
import org.junit.jupiter.api.Test;

import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class JsonDataSetReadTest
{
    @Test
    public void byRowsDataOnly()
    {
        CsvSchema schema = new CsvSchema()
                .addColumn("foo", STRING)
                .addColumn("bar", INT)
                .addColumn("baz", FLOAT)
                ;

        String jsonString = """
                [{"foo":"Alice","bar":10,"baz":123.45},\
                {"foo":"Bob","bar":12,"baz":222.33},\
                {"foo":"Carl","bar":11,"baz":323.45},\
                {"foo":"Diane","bar":14,"baz":456.78}]""";

        JsonDataSet dataSet = new JsonDataSet("data set", schema)
                .dataOnly(true)
                .dataByRows(true);

        DataFrame dataFrame = dataSet.fromJsonString(jsonString);

        DataFrameTestUtil.assertEquals(new DataFrame("expected")
                .addStringColumn("foo").addIntColumn("bar").addFloatColumn("baz")
                .addRow("Alice", 10, 123.45f)
                .addRow("Bob", 12, 222.33f)
                .addRow("Carl", 11, 323.45f)
                .addRow("Diane", 14, 456.78f)
                ,
                dataFrame);
    }

    @Test
    public void byColumnsDataOnly()
    {
        CsvSchema schema = new CsvSchema()
                .addColumn("foo", STRING)
                .addColumn("bar", INT)
                .addColumn("baz", FLOAT)
                ;

        String jsonString = """
            [\
            {"column":"foo","values":["Alice","Bob","Carl","Diane"]},\
            {"column":"bar","values":[10,12,11,14]},\
            {"column":"baz","values":[123.45,222.33,323.45,456.78]}\
            ]""";

        JsonDataSet dataSet = new JsonDataSet("data set", schema)
                .dataOnly(true)
                .dataByRows(false);

        DataFrame dataFrame = dataSet.fromJsonString(jsonString);

        DataFrameTestUtil.assertEquals(new DataFrame("expected")
                .addStringColumn("foo").addIntColumn("bar").addFloatColumn("baz")
                .addRow("Alice", 10, 123.45f)
                .addRow("Bob", 12, 222.33f)
                .addRow("Carl", 11, 323.45f)
                .addRow("Diane", 14, 456.78f)
                ,
                dataFrame);
    }

    @Test
    public void byRowsWithSchemaInJson()
    {
        String jsonString = """
            {\
            "name":"df",\
            "schema":[\
            {"Name":"foo","Type":"STRING","Stored":"Y","Expression":""},\
            {"Name":"bar","Type":"LONG","Stored":"Y","Expression":""},\
            {"Name":"baz","Type":"DOUBLE","Stored":"Y","Expression":""}\
            ],\
            "data":[\
            {"foo":"Alice","bar":10,"baz":123.45},\
            {"foo":"Bob","bar":12,"baz":222.33},\
            {"foo":"Carl","bar":11,"baz":323.45},\
            {"foo":"Diane","bar":14,"baz":456.78}\
            ]}""";

        JsonDataSet dataSet = new JsonDataSet("data set")
                .dataOnly(false)
                .dataByRows(true);

        DataFrame dataFrame = dataSet.fromJsonString(jsonString);

        DataFrameTestUtil.assertEquals(new DataFrame("expected")
                .addStringColumn("foo").addLongColumn("bar").addDoubleColumn("baz")
                .addRow("Alice", 10L, 123.45)
                .addRow("Bob", 12L, 222.33)
                .addRow("Carl", 11L, 323.45)
                .addRow("Diane", 14L, 456.78)
                ,
                dataFrame);
    }

    @Test
    public void byRowsWithSchemaInDataSet()
    {
        String jsonString = """
            {\
            "name":"frame of data",\
            "data":[\
            {"foo":"Alice","bar":10,"baz":123.45},\
            {"foo":"Bob","bar":12,"baz":222.33},\
            {"foo":"Carl","bar":11,"baz":323.45},\
            {"foo":"Diane","bar":14,"baz":456.78}\
            ]}""";

        CsvSchema schema = new CsvSchema()
                .addColumn("foo", STRING)
                .addColumn("bar", INT)
                .addColumn("baz", FLOAT)
                ;

        JsonDataSet dataSet = new JsonDataSet("data set", schema)
                .dataOnly(false)
                .dataByRows(true);

        DataFrame dataFrame = dataSet.fromJsonString(jsonString);

        assertEquals("frame of data", dataFrame.getName());

        DataFrameTestUtil.assertEquals(new DataFrame("expected")
                .addStringColumn("foo").addIntColumn("bar").addFloatColumn("baz")
                .addRow("Alice", 10, 123.45f)
                .addRow("Bob", 12, 222.33f)
                .addRow("Carl", 11, 323.45f)
                .addRow("Diane", 14, 456.78f)
                ,
                dataFrame);
    }

    @Test
    public void byColumnsWithSchemaInJson()
    {
        String jsonString = """
            {\
            "name":"df",\
            "schema":[\
            {"Name":"foo","Type":"STRING","Stored":"Y","Expression":""},\
            {"Name":"bar","Type":"LONG","Stored":"Y","Expression":""},\
            {"Name":"baz","Type":"DOUBLE","Stored":"Y","Expression":""}\
            ],\
            "data":[\
            {"column":"foo","values":["Alice","Bob","Carl","Diane"]},\
            {"column":"bar","values":[10,12,11,14]},\
            {"column":"baz","values":[123.45,222.33,323.45,456.78]}\
            ]}""";

        JsonDataSet dataSet = new JsonDataSet("data set")
                .dataOnly(false)
                .dataByRows(false);

        DataFrame dataFrame = dataSet.fromJsonString(jsonString);

        DataFrameTestUtil.assertEquals(new DataFrame("expected")
                .addStringColumn("foo").addLongColumn("bar").addDoubleColumn("baz")
                .addRow("Alice", 10L, 123.45)
                .addRow("Bob", 12L, 222.33)
                .addRow("Carl", 11L, 323.45)
                .addRow("Diane", 14L, 456.78)
                ,
                dataFrame);
    }

    @Test
    public void byColumnsWithSchemaInDataSet()
    {
        String jsonString = """
            {\
            "name":"frame of data",\
            "data":[\
            {"column":"foo","values":["Alice","Bob","Carl","Diane"]},\
            {"column":"bar","values":[10,12,11,14]},\
            {"column":"baz","values":[123.45,222.33,323.45,456.78]}\
            ]}""";

        CsvSchema schema = new CsvSchema()
                .addColumn("foo", STRING)
                .addColumn("bar", LONG)
                .addColumn("baz", DOUBLE)
                ;

        JsonDataSet dataSet = new JsonDataSet("data set", schema)
                .dataOnly(false)
                .dataByRows(false);

        DataFrame dataFrame = dataSet.fromJsonString(jsonString);

        DataFrameTestUtil.assertEquals(new DataFrame("expected")
                .addStringColumn("foo").addLongColumn("bar").addDoubleColumn("baz")
                .addRow("Alice", 10L, 123.45)
                .addRow("Bob", 12L, 222.33)
                .addRow("Carl", 11L, 323.45)
                .addRow("Diane", 14L, 456.78)
                ,
                dataFrame);

        assertEquals("frame of data", dataFrame.getName());
    }

    @Test
    public void dataFrameSchemaAsJsonStringToDataFrame()
    {
        String jsonString = """
            [\
            {"Name":"foo","Type":"STRING","Stored":"Y","Expression":""},\
            {"Name":"bar","Type":"LONG","Stored":"Y","Expression":""},\
            {"Name":"baz","Type":"DOUBLE","Stored":"Y","Expression":""},\
            {"Name":"twoBaz","Type":"DOUBLE","Stored":"N","Expression":"baz * 2.0"}\
            ]""";

        CsvSchema schema = new CsvSchema()
                .addColumn("Name", STRING)
                .addColumn("Type", STRING)
                .addColumn("Stored", STRING)
                .addColumn("Expression", STRING);

        JsonDataSet dataSet = new JsonDataSet("df schema", schema)
                .dataByRows(true)
                .dataOnly(true);

        DataFrame schemaDataFrame = dataSet.fromJsonString(jsonString);

        DataFrameTestUtil.assertEquals(new DataFrame("expected")
            .addStringColumn("Name").addStringColumn("Type").addStringColumn("Stored").addStringColumn("Expression")
            .addRow("foo", "STRING", "Y", "")
            .addRow("bar", "LONG", "Y", "")
            .addRow("baz", "DOUBLE", "Y", "")
            .addRow("twoBaz", "DOUBLE", "N", "baz * 2.0")
            ,
            schemaDataFrame);
    }
}
