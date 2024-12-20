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

public class JsonDataSetNullHandlingTest
{
    private DataFrame dataFrame;

    @BeforeEach
    public void setUpDataFrame()
    {
        this.dataFrame = new DataFrame("df")
            .addStringColumn("aString").addLongColumn("aLong").addDoubleColumn("aDouble").addIntColumn("anInt")
            .addFloatColumn("aFloat").addDateColumn("aDate").addDateTimeColumn("aDateTime").addDecimalColumn("aDecimal")
            .addBooleanColumn("aBoolean")
            .addRow("Alice", 10L, 123.45, null, 12.34f, LocalDate.of(2024, 11, 12), LocalDateTime.of(2024, 11, 12, 20, 38, 45),                       null,  true)
            .addRow("Bob",  null, 222.33,   15,   null,  LocalDate.of(2024, 9, 22),                                       null, BigDecimal.valueOf(123, 2), false)
            .addRow(null,    12L,   null,   16, 55.34f,                       null, LocalDateTime.of(2024, 10, 25, 20, 38, 45), BigDecimal.valueOf(456, 1),  null)
        ;
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
                        {"aString":"Alice","aLong":10,"aDouble":123.45,"anInt":null,"aFloat":12.34,"aDate":"2024-11-12","aDateTime":"2024-11-12T20:38:45","aDecimal":null,"aBoolean":true},\
                        {"aString":"Bob","aLong":null,"aDouble":222.33,"anInt":15,"aFloat":null,"aDate":"2024-09-22","aDateTime":null,"aDecimal":1.23,"aBoolean":false},\
                        {"aString":null,"aLong":12,"aDouble":null,"anInt":16,"aFloat":55.34,"aDate":null,"aDateTime":"2024-10-25T20:38:45","aDecimal":45.6,"aBoolean":null}\
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
                {"column":"aString","values":["Alice","Bob",null]},\
                {"column":"aLong","values":[10,null,12]},\
                {"column":"aDouble","values":[123.45,222.33,null]},\
                {"column":"anInt","values":[null,15,16]},\
                {"column":"aFloat","values":[12.34,null,55.34]},\
                {"column":"aDate","values":["2024-11-12","2024-09-22",null]},\
                {"column":"aDateTime","values":["2024-11-12T20:38:45",null,"2024-10-25T20:38:45"]},\
                {"column":"aDecimal","values":[null,1.23,45.6]},\
                {"column":"aBoolean","values":[true,false,null]}\
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
