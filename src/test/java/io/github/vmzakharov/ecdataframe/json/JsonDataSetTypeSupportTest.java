package io.github.vmzakharov.ecdataframe.json;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataset.CsvSchema;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class JsonDataSetTypeSupportTest
{
    private DataFrame dataFrame;

    @BeforeEach
    public void setUpDataFrame()
    {
        this.dataFrame = new DataFrame("df")
            .addStringColumn("aString").addLongColumn("aLong").addDoubleColumn("aDouble").addIntColumn("anInt")
            .addFloatColumn("aFloat").addDateColumn("aDate").addDateTimeColumn("aDateTime").addDecimalColumn("aDecimal")
            .addBooleanColumn("aBoolean")
            .addRow("Alice", 10L, 123.45, 11, 12.34f, LocalDate.of(2024, 11, 12), LocalDateTime.of(2024, 11, 12, 20, 38, 45), BigDecimal.valueOf(123, 2), true)
            .addRow("Bob", 12L, 222.33, 15, 55.34f, LocalDate.of(2024, 9, 22), LocalDateTime.of(2024, 10, 25, 20, 38, 45), BigDecimal.valueOf(456, 1), false);
    }

    @Test
    public void readWriteByRows()
    {
        JsonDataSet dataSetByRow = new JsonDataSet("json")
                .dataByRows(true)
                .dataOnly(true);

        String jsonString = dataSetByRow.toJsonString(this.dataFrame);

        Assertions.assertEquals("""
                        [\
                        {"aString":"Alice","aLong":10,"aDouble":123.45,"anInt":11,"aFloat":12.34,"aDate":"2024-11-12","aDateTime":"2024-11-12T20:38:45","aDecimal":1.23,"aBoolean":true},\
                        {"aString":"Bob","aLong":12,"aDouble":222.33,"anInt":15,"aFloat":55.34,"aDate":"2024-09-22","aDateTime":"2024-10-25T20:38:45","aDecimal":45.6,"aBoolean":false}\
                        ]"""
                , jsonString);

        CsvSchema schema = new CsvSchema()
                .addColumn("aString", ValueType.STRING)
                .addColumn("aLong", ValueType.LONG)
                .addColumn("aDouble", ValueType.DOUBLE)
                .addColumn("anInt", ValueType.INT)
                .addColumn("aFloat", ValueType.FLOAT)
                .addColumn("aDate", ValueType.DATE)
                .addColumn("aDateTime", ValueType.DATE_TIME)
                .addColumn("aDecimal", ValueType.DECIMAL)
                .addColumn("aBoolean", ValueType.BOOLEAN);

        JsonDataSet dataSetRead = new JsonDataSet("json", schema)
                .dataByRows(true)
                .dataOnly(true);

        DataFrame dataFrameRead = dataSetRead.fromJsonString(jsonString);

        DataFrameTestUtil.assertEquals(this.dataFrame, dataFrameRead);
    }

    @Test
    public void readWriteByCols()
    {
        JsonDataSet dataSetByCol = new JsonDataSet("json")
                .dataByRows(false)
                .dataOnly(true);

        String jsonString = dataSetByCol.toJsonString(this.dataFrame);

        Assertions.assertEquals("""
                [\
                {"column":"aString","values":["Alice","Bob"]},\
                {"column":"aLong","values":[10,12]},\
                {"column":"aDouble","values":[123.45,222.33]},\
                {"column":"anInt","values":[11,15]},\
                {"column":"aFloat","values":[12.34,55.34]},\
                {"column":"aDate","values":["2024-11-12","2024-09-22"]},\
                {"column":"aDateTime","values":["2024-11-12T20:38:45","2024-10-25T20:38:45"]},\
                {"column":"aDecimal","values":[1.23,45.6]},\
                {"column":"aBoolean","values":[true,false]}\
                ]"""
                , jsonString);

        CsvSchema schema = new CsvSchema()
                .addColumn("aString", ValueType.STRING)
                .addColumn("aLong", ValueType.LONG)
                .addColumn("aDouble", ValueType.DOUBLE)
                .addColumn("anInt", ValueType.INT)
                .addColumn("aFloat", ValueType.FLOAT)
                .addColumn("aDate", ValueType.DATE)
                .addColumn("aDateTime", ValueType.DATE_TIME)
                .addColumn("aDecimal", ValueType.DECIMAL)
                .addColumn("aBoolean", ValueType.BOOLEAN);

        JsonDataSet dataSetRead = new JsonDataSet("json", schema)
                .dataByRows(false)
                .dataOnly(true);

        DataFrame dataFrameRead = dataSetRead.fromJsonString(jsonString);

        DataFrameTestUtil.assertEquals(this.dataFrame, dataFrameRead);
    }
}
