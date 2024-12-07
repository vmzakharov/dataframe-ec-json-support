package io.github.vmzakharov.ecdataframe.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DfBooleanColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfDateColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfDateTimeColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfDecimalColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfDoubleColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfFloatColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfIntColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfLongColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfStringColumn;
import io.github.vmzakharov.ecdataframe.dataset.CsvSchema;
import io.github.vmzakharov.ecdataframe.dataset.CsvSchemaColumn;
import io.github.vmzakharov.ecdataframe.dataset.DataSetAbstract;
import io.github.vmzakharov.ecdataframe.dsl.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DateTimeValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DateValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DecimalValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DoubleValue;
import io.github.vmzakharov.ecdataframe.dsl.value.FloatValue;
import io.github.vmzakharov.ecdataframe.dsl.value.IntValue;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import io.github.vmzakharov.ecdataframe.util.ExceptionFactory;
import org.eclipse.collections.api.block.procedure.Procedure;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.Maps;

import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.STRING;
import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exceptionByKey;

public class JsonDataSet
        extends DataSetAbstract
{
    private boolean dataByRows = true;
    private boolean schemaIncluded = false;
    private boolean dataOnly = false;

    private CsvSchema schema;

    public JsonDataSet(String newName)
    {
        super(newName);
    }

    public JsonDataSet(String newName, CsvSchema newSchema)
    {
        super(newName);
        this.schema = newSchema;
    }

    public boolean dataByRows()
    {
        return this.dataByRows;
    }

    public boolean dataByColumns()
    {
        return !this.dataByRows();
    }

    /**
     * Specifies whether the data representing the data frame is organized by rows or by columns in the json object.
     * If the data is organized by rows, the portion of the json object representing the data frame data is a Json
     * array, which in turn contains Json objects representing the rows of the data frame. Each row
     * object contains column names and their values for that row.
     * If the data is organized by columns, the data frame data is stored as a Json array containing Json objects
     * representing columns of the data frame. Each column object contains the column name and an array of column
     * values.
     *
     * @param byRows if true, the data is organized by rows, if false the data is organized by columns
     * @return this data set
     */
    public JsonDataSet dataByRows(boolean byRows)
    {
        this.dataByRows = byRows;
        return this;
    }

    public boolean schemaIncluded()
    {
        return this.schemaIncluded;
    }

    /**
     * Specifies whether the data frame schema is included in the json representation of the data frame.
     *
     * @param newSchemaIncluded true if the data frame schema is stored in the json object, false otherwise
     * @return this data set
     */
    public JsonDataSet schemaIncluded(boolean newSchemaIncluded)
    {
        this.schemaIncluded = newSchemaIncluded;
        return this;
    }

    public boolean dataOnly()
    {
        return this.dataOnly;
    }

    /**
     * Specifies whether to only store data frame data in the json object or to also include metadata, like data frame
     * name, schema, etc.
     * NOTE: if newDataOnly value is set to {@code true} the value of the {@code schemaIncluded} flag is ignored
     *
     * @param newDataOnly true if only data is stored in the json object, false otherwise
     * @return this data set
     */
    public JsonDataSet dataOnly(boolean newDataOnly)
    {
        this.dataOnly = newDataOnly;
        return this;
    }

    @Override
    public void openFileForReading()
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Object next()
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean hasNext()
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void close()
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private boolean schemaIsNotDefined()
    {
        return this.schema == null;
    }

    /**
     * De-serialize a data frame from a Json string based on the parameters of the data set (by rows, by columns, based
     * on the included schema, etc.)
     * If the json object does not have schema specified, the schema must be explicitly provided with the data set
     *
     * @param jsonString Json string containing the data in the data frame organized as described by the properties of
     *                   the data set
     * @return a data frame populated with the data in the Json object passes as the parameter
     */
    public DataFrame fromJsonString(String jsonString)
    {
        JsonNode topNode = this.readTree(jsonString);

        return this.fromJsonObject(topNode);
    }

        /**
         * De-serialize a data frame from a Json string based on the parameters of the data set (by rows, by columns, based
         * on the included schema, etc.)
         * If the json object does not have schema specified, the schema must be explicitly provided with the data set
         *
         * @param topNode a Json node object containing the data in the data frame organized as described by the properties
         *                of the data set
         * @return a data frame populated with the data in the Json object passes as the parameter
         */
    public DataFrame fromJsonObject(JsonNode topNode)
    {
        MutableList<Procedure<String>> columnPopulators = Lists.mutable.of();

        JsonNode dataNode;

        if (this.dataOnly())
        {
            this.validateSchemaIsDefinedOrThrow();

            dataNode = topNode;
        }
        else
        {
            JsonNode schemaNode = topNode.get("schema");

            if (schemaNode == null)
            {
                this.validateSchemaIsDefinedOrThrow();
            }
            else
            {
                this.populateSchemaFromSchemaNode(schemaNode);
            }

            dataNode = topNode.get("data");
        }

        DataFrame dataFrame = new DataFrame(this.dataOnly() ? this.getName() : topNode.get("name").asText());

        this.schema.getColumns()
                   .forEach(col -> this.addDataFrameColumn(dataFrame, col, columnPopulators));

        this.populateDataFrameWithData(columnPopulators, dataNode);

        dataFrame.seal();

        return dataFrame;
    }

    private void validateSchemaIsDefinedOrThrow()
    {
        if (this.schemaIsNotDefined())
        {
            throw new RuntimeException("When reading a Json object, schema must be specified in the data set definition or in the json string");
        }
    }

    private void populateSchemaFromSchemaNode(JsonNode schemaNode)
    {
        DataFrame schemaDataFrame = this.schemaDataFrame(schemaNode);

        this.schema = new CsvSchema();
        schemaDataFrame
                .selectBy("Stored == 'Y'")
                .forEach(
                        row -> this.schema.addColumn(
                                row.getString("Name"),
                                ValueType.valueOf(row.getString("Type"))
                        )
                );
    }

    private void populateDataFrameWithData(MutableList<Procedure<String>>  columnPopulators, JsonNode dataNode)
    {
        if (dataNode instanceof ArrayNode arrayNode)
        {
            if (this.dataByRows())
            {
                this.populateDataFrameFromJsonRows(arrayNode, columnPopulators);
            }
            else
            {
                this.populateDataFrameFromJsonColumns(arrayNode, columnPopulators);
            }
        }
        else
        {
            throw new RuntimeException("Unexpected data node type: " + dataNode.getClass().getSimpleName());
        }
    }

    private DataFrame schemaDataFrame(JsonNode schemaNode)
    {
        CsvSchema schemaSchema = new CsvSchema()
                .addColumn("Name", STRING)
                .addColumn("Type", STRING)
                .addColumn("Stored", STRING)
                .addColumn("Expression", STRING);

        JsonDataSet dataSet = new JsonDataSet("df schema", schemaSchema)
                .dataByRows(true)
                .dataOnly(true);

        return dataSet.fromJsonObject(schemaNode);
    }

    private void populateDataFrameFromJsonColumns(ArrayNode columns, MutableList<Procedure<String>> columnPopulators)
    {
        MutableMap<String, Procedure<String>> columnPopulatorsByName = Maps.mutable.of();

        this.schema.getColumns()
                 .forEachInBoth(columnPopulators,
                     (col, populator) -> columnPopulatorsByName.put(col.getName(), populator));

        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++)
        {
            JsonNode columnObject = columns.get(columnIndex);
            String columnName = columnObject.get("column").asText();
            ArrayNode columnValues = (ArrayNode) columnObject.get("values");

            Procedure<String> columnPopulator = columnPopulatorsByName.get(columnName);

            for (int valueIndex = 0; valueIndex < columnValues.size(); valueIndex++)
            {
                columnPopulator.value(columnValues.get(valueIndex).asText());
            }
        }
    }

    private void populateDataFrameFromJsonRows(ArrayNode rows, MutableList<Procedure<String>> columnPopulators)
    {
        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++)
        {
            JsonNode rowObject = rows.get(rowIndex);
            this.schema.getColumns()
                       .forEachInBoth(columnPopulators,
                           (col, populator) -> populator.value(rowObject.get(col.getName()).asText())
                       );
        }
    }

    private void addDataFrameColumn(DataFrame df, CsvSchemaColumn schemaCol, MutableList<Procedure<String>> columnPopulators)
    {
        ValueType columnType = schemaCol.getType();

        DfColumn lastColumn = df.newColumn(schemaCol.getName(), columnType);

        Procedure<String> populator = switch (columnType)
        {
            case LONG -> s -> schemaCol.parseAsLongAndAdd(s, lastColumn);
            case DOUBLE -> s -> schemaCol.parseAsDoubleAndAdd(s, lastColumn);
            case INT -> s -> schemaCol.parseAsIntAndAdd(s, lastColumn);
            case FLOAT -> s -> schemaCol.parseAsFloatAndAdd(s, lastColumn);
            case BOOLEAN -> s -> schemaCol.parseAsBooleanAndAdd(s, lastColumn);
            case STRING -> s -> lastColumn.addObject(schemaCol.parseAsString(s));
            case DATE -> s -> lastColumn.addObject(schemaCol.parseAsLocalDate(s));
            case DATE_TIME -> s -> lastColumn.addObject(schemaCol.parseAsLocalDateTime(s));
            case DECIMAL -> s -> lastColumn.addObject(schemaCol.parseAsDecimal(s));
            default -> throw exceptionByKey("CSV_POPULATING_BAD_COL_TYPE").with("columnType", columnType)
                                                                          .get();
        };

        columnPopulators.add(populator);
    }

    public DataFrame dataFrameWithJsonSchema(String json)
    {
        DataFrame schemaDf = this.fromJsonString(json);

        DataFrame dataFrame = new DataFrame("from json");

        schemaDf.forEach(row -> {
                if (row.getString("Stored").equals("Y"))
                {
                    dataFrame.addColumn(row.getString("Name"), ValueType.valueOf(row.getString("Type")));
                }
                else
                {
                    dataFrame.addColumn(row.getString("Name"), row.getString("Expression"));
                }
            }
        );

        return dataFrame;
    }

    private JsonNode readTree(String jsonString)
    {
        try
        {
            return new ObjectMapper().readTree(jsonString);
        }
        catch (JsonProcessingException e)
        {
            throw new RuntimeException("Ay Carrumba!", e);
        }
    }

    /**
     * Serialize a data frame into a Json string based on the parameters of the data set (by rows, by columns, include
     * schema, etc.)
     *
     * @param dataFrame the data frame to serialize to Json
     * @return Json string containing the data in the data frame organized as described by the properties of the data
     * set
     */
    public String toJsonString(DataFrame dataFrame)
    {
        JsonNode dataNode = this.dataByColumns() ? this.toJsonNodeByColumns(dataFrame) : this.toJsonNodeByRows(dataFrame);

        if (this.dataOnly())
        {
            return dataNode.toString();
        }

        ObjectNode dfAsJson = new ObjectMapper().createObjectNode();

        dfAsJson.put("name", dataFrame.getName());

        if (this.schemaIncluded())
        {
            dfAsJson.set("schema", this.toJsonNodeByRows(dataFrame.schema()));
        }

        dfAsJson.set("data", dataNode);

        return dfAsJson.toString();
    }

    private JsonNode toJsonNodeByRows(DataFrame dataFrame)
    {
        ArrayNode dfByRows = new ObjectMapper().createArrayNode();

        ImmutableList<DfColumn> columns = dataFrame.getColumns();

        dataFrame.forEach(
                row -> {
                    ObjectNode rowNode = new ObjectMapper().createObjectNode();
                    dfByRows.add(rowNode);
                    columns.forEach(col ->
                            this.addValueToRowNode(rowNode, col, row.rowIndex())
                    );
                }
        );

        return dfByRows;
    }

    public void addValueToRowNode(ObjectNode rowNode, DfColumn column, int rowIndex)
    {
        Value value = column.getValue(rowIndex);
        String name = column.getName();

        switch (value.getType())
        {
            case LONG -> rowNode.put(name, ((LongValue) value).longValue());
            case INT -> rowNode.put(name, ((IntValue) value).intValue());
            case FLOAT -> rowNode.put(name, ((FloatValue) value).floatValue());
            case DOUBLE -> rowNode.put(name, ((DoubleValue) value).doubleValue());
            case DECIMAL -> rowNode.put(name, ((DecimalValue) value).decimalValue());
            case STRING -> rowNode.put(name, value.stringValue());
            case BOOLEAN -> rowNode.put(name, ((BooleanValue) value).isTrue());
            case DATE -> rowNode.put(name, ((DateValue) value).asStringLiteral());
            case DATE_TIME -> rowNode.put(name, ((DateTimeValue) value).asStringLiteral());
            default -> throw this.getUnsupportedColumnException(column);
        }
    }

    private JsonNode toJsonNodeByColumns(DataFrame dataFrame)
    {
        ImmutableList<DfColumn> columns = dataFrame.getColumns();

        ArrayNode dataNode = new ObjectMapper().createArrayNode();

        columns.forEach(column -> this.addColumnValues(dataNode, column));

        return dataNode;
    }

    private void addColumnValues(ArrayNode arrayNode, DfColumn column)
    {
        ObjectNode columnNode = new ObjectMapper().createObjectNode();
        columnNode.put("column", column.getName());

        ArrayNode values = new ObjectMapper().createArrayNode();

        switch (column.getType())
        {
            case LONG -> ((DfLongColumn) column).asLongIterable().forEach(values::add);
            case INT -> ((DfIntColumn) column).asIntIterable().forEach(values::add);
            case FLOAT -> ((DfFloatColumn) column).asFloatIterable().forEach(values::add);
            case DOUBLE -> ((DfDoubleColumn) column).asDoubleIterable().forEach(values::add);
            case DECIMAL -> ((DfDecimalColumn) column).toList().forEach(values::add);
            case STRING -> ((DfStringColumn) column).toList().forEach(values::add);
            case BOOLEAN -> ((DfBooleanColumn) column).toBooleanList().forEach(values::add);
            case DATE -> ((DfDateColumn) column).toList().forEach(each -> values.add(each.toString()));
            case DATE_TIME -> ((DfDateTimeColumn) column).toList().forEach(each -> values.add(each.toString()));
            default -> throw this.getUnsupportedColumnException(column);
        }

        columnNode.set("values", values);

        arrayNode.add(columnNode);
    }

    private RuntimeException getUnsupportedColumnException(DfColumn column)
    {
        return ExceptionFactory
                .exception("Cannot convert values in column " + column.getName() + " of type " + column.getType() + " to Json")
                .getUnsupported();
    }
}
