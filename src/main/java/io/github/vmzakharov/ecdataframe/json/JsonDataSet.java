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
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import io.github.vmzakharov.ecdataframe.util.ExceptionFactory;
import org.eclipse.collections.api.block.procedure.Procedure;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.Maps;

import java.util.function.IntConsumer;

import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.BOOLEAN;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.DATE;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.DATE_TIME;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.DECIMAL;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.DOUBLE;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.FLOAT;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.INT;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.LONG;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.STRING;
import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exceptionByKey;

/**
 * The main class for data frame JSON serialization support. It supports serializing and deserializing data frames
 * to/from JSON strings structured as follows:
 * <ul>
 *   <li>Data frame data organized by rows or by columns</li>
 *   <li>Includes just the data frame data or both data and metadata</li>
 *   <li>Metadata (if specified) can include Data frame name and (optionally) Data frame schema</li>
 * </ul>
 */
public class JsonDataSet
extends DataSetAbstract
{
    static private final ListIterable<ValueType> SUPPORTED_TYPES = Lists.immutable.with(
        LONG, DOUBLE, INT, FLOAT, BOOLEAN, STRING, DATE, DATE_TIME, DECIMAL
    );

    private boolean dataByRows = true;
    private boolean schemaIncluded = false;
    private boolean dataOnly = false;

    private CsvSchema schema;

    /**
     * Create a new instance of a JSON data set
     * @param newName the name of the newly created data set
     */
    public JsonDataSet(String newName)
    {
        super(newName);
    }

    /**
     * Create a new instance of a JSON data set
     * @param newName the name of the newly created data set
     * @param newSchema the schema describing the dataframe and/or data in the JSON object
     */
    public JsonDataSet(String newName, CsvSchema newSchema)
    {
        super(newName);
        this.schema = newSchema;
    }

    /**
     * Indicates if the data frame data is organized by rows in the JSON object
     * @return {@code true} if the data frame data is organized by rows in the JSON object, {@code false} if the data
     * is organized by columns
     */
    public boolean dataByRows()
    {
        return this.dataByRows;
    }

    /**
     * Indicates if the data frame data is organized by columns in the JSON object
     * @return {@code true} if the data frame data is organized by columns in the JSON object, {@code false} if the data
     * is organized by columns
     */
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

    /**
     * @return {@code true} if the data frame schema is included in the metadata stored in the JSON object,
     * {@code false} otherwise
     */
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

    /**
     * @return {@code true} if the data frame metadata stored in the JSON object, {@code false} otherwise
     */
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
        throw this.notYetSupportedException();
    }

    @Override
    public Object next()
    {
        throw this.notYetSupportedException();
    }

    @Override
    public boolean hasNext()
    {
        throw this.notYetSupportedException();
    }

    @Override
    public void close()
    {
        throw this.notYetSupportedException();
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
            throw ExceptionFactory
                    .exception("When reading a Json object, schema must be specified in the data set definition or in the json string")
                    .get();
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
            throw ExceptionFactory.exception("Unexpected data node type, expected array node: " + dataNode.getClass().getSimpleName()).get();
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
                columnPopulator.value(this.nodeTextOrNull(columnValues.get(valueIndex)));
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
                               (col, populator) -> populator.value(this.nodeTextOrNull(rowObject.get(col.getName())))
                       );
        }
    }

    private String nodeTextOrNull(JsonNode node)
    {
        return node.isNull() ? null : node.asText();
    }

    private void addDataFrameColumn(DataFrame df, CsvSchemaColumn schemaCol, MutableList<Procedure<String>> columnPopulators)
    {
        ValueType columnType = schemaCol.getType();

        DfColumn lastColumn = df.newColumn(schemaCol.getName(), columnType);

        if (SUPPORTED_TYPES.contains(columnType))
        {
            Procedure<String> populator = s -> schemaCol.parseAndAddToColumn(s, lastColumn);
            columnPopulators.add(populator);
        }
        else
        {
            throw exceptionByKey("CSV_POPULATING_BAD_COL_TYPE").with("columnType", columnType).get();
        }
    }

    private JsonNode readTree(String jsonString)
    {
        try
        {
            return new ObjectMapper().readTree(jsonString);
        }
        catch (JsonProcessingException e)
        {
            throw ExceptionFactory.exception("Failed to parse JSON string").get(e);
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

    private void addValueToRowNode(ObjectNode rowNode, DfColumn column, int rowIndex)
    {
        String name = column.getName();

        if (column.isNull(rowIndex))
        {
            rowNode.putNull(name);
        }
        else
        {
            switch (column.getType())
            {
                case LONG -> rowNode.put(name, ((DfLongColumn) column).getLong(rowIndex));
                case INT -> rowNode.put(name, ((DfIntColumn) column).getInt(rowIndex));
                case FLOAT -> rowNode.put(name, ((DfFloatColumn) column).getFloat(rowIndex));
                case DOUBLE -> rowNode.put(name, ((DfDoubleColumn) column).getDouble(rowIndex));
                case DECIMAL -> rowNode.put(name, ((DfDecimalColumn) column).getTypedObject(rowIndex));
                case STRING -> rowNode.put(name, column.getValueAsString(rowIndex));
                case BOOLEAN -> rowNode.put(name, ((DfBooleanColumn) column).getBoolean(rowIndex));
                case DATE, DATE_TIME -> rowNode.put(name, column.getValueAsStringLiteral(rowIndex));
                default -> throw this.unsupportedColumnException(column);
            }
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

        int columnSize = column.getSize();

        IntConsumer addBlock;

        if (column instanceof DfLongColumn longColumn)
        {
            addBlock = rowIndex -> values.add(longColumn.getLong(rowIndex));
        }
        else if (column instanceof DfIntColumn intColumn)
        {
            addBlock = rowIndex -> values.add(intColumn.getInt(rowIndex));
        }
        else if (column instanceof DfFloatColumn floatColumn)
        {
            addBlock = rowIndex -> values.add(floatColumn.getFloat(rowIndex));
        }
        else if (column instanceof DfDoubleColumn doubleColumn)
        {
            addBlock = rowIndex -> values.add(doubleColumn.getDouble(rowIndex));
        }
        else if (column instanceof DfBooleanColumn booleanColumn)
        {
            addBlock = rowIndex -> values.add(booleanColumn.getBoolean(rowIndex));
        }
        else if (column instanceof DfStringColumn stringColumn)
        {
            addBlock = rowIndex -> values.add(stringColumn.getTypedObject(rowIndex));
        }
        else if (column instanceof DfDateColumn dateColumn)
        {
            addBlock = rowIndex -> values.add(dateColumn.getValueAsStringLiteral(rowIndex));
        }
        else if (column instanceof DfDateTimeColumn dateTimeColumn)
        {
            addBlock = rowIndex -> values.add(dateTimeColumn.getValueAsStringLiteral(rowIndex));
        }
        else if (column instanceof DfDecimalColumn decimalColumn)
        {
            addBlock = rowIndex -> values.add(decimalColumn.getTypedObject(rowIndex));
        }
        else
        {
            throw this.unsupportedColumnException(column);
        }

        for (int rowIndex = 0; rowIndex < columnSize; rowIndex++)
        {
            if (column.isNull(rowIndex))
            {
                values.addNull();
            }
            else
            {
                addBlock.accept(rowIndex);
            }
        }

        columnNode.set("values", values);

        arrayNode.add(columnNode);
    }

    private RuntimeException notYetSupportedException()
    {
        return ExceptionFactory.exception("Not supported yet.").getUnsupported();
    }

    private RuntimeException unsupportedColumnException(DfColumn column)
    {
        return ExceptionFactory
                .exception("Cannot convert values in column " + column.getName() + " of type " + column.getType() + " to Json")
                .getUnsupported();
    }
}
