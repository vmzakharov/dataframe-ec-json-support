package io.github.vmzakharov.ecdataframe.json;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DataFrameToJsonTest
{
    private DataFrame dataFrame;

    @BeforeEach
    public void setUpDataFrame()
    {
        this.dataFrame = new DataFrame("df")
                .addStringColumn("foo").addLongColumn("bar").addDoubleColumn("baz")
                .addRow("Alice", 10L, 123.45)
                .addRow("Bob", 12L, 222.33)
                .addRow("Carl", 11L, 323.45)
                .addRow("Diane", 14L, 456.78)
        ;
    }

    @Test
    public void byRowsAsDataArray()
    {
        DataFrameToJson dfToJson = new DataFrameToJson();

        String jsonString = dfToJson.toJsonStringByRowsAsDataArray(this.dataFrame);

        assertEquals(
                """
                [{"foo":"Alice","bar":10,"baz":123.45},\
                {"foo":"Bob","bar":12,"baz":222.33},\
                {"foo":"Carl","bar":11,"baz":323.45},\
                {"foo":"Diane","bar":14,"baz":456.78}]""",
                jsonString
        );
    }

    @Test
    public void byRowsNoSchema()
    {
        DataFrameToJson dfToJson = new DataFrameToJson();

        String jsonString = dfToJson.toJsonStringByRows(this.dataFrame, false);

        assertEquals(
            """
            {\
            "name":"df",\
            "data":[\
            {"foo":"Alice","bar":10,"baz":123.45},\
            {"foo":"Bob","bar":12,"baz":222.33},\
            {"foo":"Carl","bar":11,"baz":323.45},\
            {"foo":"Diane","bar":14,"baz":456.78}\
            ]}""",
            jsonString
        );
    }

    @Test
    public void byRowsWithSchema()
    {
        DataFrameToJson dfToJson = new DataFrameToJson();

        String jsonString = dfToJson.toJsonStringByRows(this.dataFrame, true);

        assertEquals(
            """
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
            ]}""",
            jsonString
        );
    }

    @Test
    public void byColumnsAsDataArray()
    {
        DataFrameToJson dfToJson = new DataFrameToJson();

        String jsonString = dfToJson.toJsonStringByColumnsAsDataArray(this.dataFrame);

        assertEquals(
            """
            [\
            {"column":"foo","values":["Alice","Bob","Carl","Diane"]},\
            {"column":"bar","values":[10,12,11,14]},\
            {"column":"baz","values":[123.45,222.33,323.45,456.78]}\
            ]""",
            jsonString
        );
    }

    @Test
    public void byColumnsNoSchema()
    {
        DataFrameToJson dfToJson = new DataFrameToJson();

        String jsonString = dfToJson.toJsonStringByColumns(this.dataFrame, false);

        assertEquals(
            """
            {"name":"df",\
            "data":[\
            {"column":"foo","values":["Alice","Bob","Carl","Diane"]},\
            {"column":"bar","values":[10,12,11,14]},\
            {"column":"baz","values":[123.45,222.33,323.45,456.78]}\
            ]}""",
            jsonString
        );
    }

    @Test
    public void byColumnsWithSchema()
    {
        DataFrameToJson dfToJson = new DataFrameToJson();

        String jsonString = dfToJson.toJsonStringByColumns(this.dataFrame, true);

        assertEquals(
                """
                {"name":"df",\
                "schema":[\
                {"Name":"foo","Type":"STRING","Stored":"Y","Expression":""},\
                {"Name":"bar","Type":"LONG","Stored":"Y","Expression":""},\
                {"Name":"baz","Type":"DOUBLE","Stored":"Y","Expression":""}\
                ],\
                "data":[\
                {"column":"foo","values":["Alice","Bob","Carl","Diane"]},\
                {"column":"bar","values":[10,12,11,14]},\
                {"column":"baz","values":[123.45,222.33,323.45,456.78]}\
                ]}""",
                jsonString
        );
    }

    @Test
    public void serializingSchema()
    {
        DataFrame schema = this.dataFrame.schema();

        DataFrameToJson dfToJson = new DataFrameToJson();

        String jsonString = dfToJson.toJsonStringByRowsAsDataArray(schema);

        assertEquals(
            """
            [\
            {"Name":"foo","Type":"STRING","Stored":"Y","Expression":""},\
            {"Name":"bar","Type":"LONG","Stored":"Y","Expression":""},\
            {"Name":"baz","Type":"DOUBLE","Stored":"Y","Expression":""}\
            ]""",
            jsonString
        );
    }
}
