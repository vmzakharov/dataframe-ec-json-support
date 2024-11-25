package io.github.vmzakharov.ecdataframe.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.github.vmzakharov.ecdataframe.dataframe.*;
import io.github.vmzakharov.ecdataframe.dsl.value.*;
import io.github.vmzakharov.ecdataframe.util.ExceptionFactory;
import org.eclipse.collections.api.list.ImmutableList;

public class DataFrameToJson
{
    /**
     * Serialize a data frame into a Json string with the data organized by columns. The top level object is a Json
     * array containing Json objects representing columns of the data frame
     * @param dataFrame the data frame to serialize to Json
     * @return Json string containing the data in the data frame organized by column. Each array element contains
     * the column name and column data
     */
    public String toJsonStringByRowsAsDataArray(DataFrame dataFrame)
    {
        return this.toJsonNodeByRows(dataFrame).toString();
    }

    /**
     * Serialize a data frame into a Json string with the data organized by columns
     * @param dataFrame the data frame to serialize to Json
     * @param includeSchema embed the json representation of the schema of this data frame
     * @return Json string containing the data in the data frame organized by column and (optionally) the data frame
     *         schema.
     */
    public String toJsonStringByRows(DataFrame dataFrame, boolean includeSchema)
    {
        ObjectNode dfAsJson = new ObjectMapper().createObjectNode();

        dfAsJson.put("name", dataFrame.getName());

        if (includeSchema)
        {
            dfAsJson.set("schema", this.toJsonNodeByRows(dataFrame.schema()));
        }

        dfAsJson.set("data", this.toJsonNodeByRows(dataFrame));

        return dfAsJson.toString();
    }

    /**
     * Serialize a data frame into a Json string with the data organized by columns. The top level object is a Json
     * array containing Json objects representing columns of the data frame
     * @param dataFrame the data frame to serialize to Json
     * @return Json string containing the data in the data frame organized by column. Each array element contains
     * the column name and column data
     */
    public String toJsonStringByColumnsAsDataArray(DataFrame dataFrame)
    {
        return this.toJsonNodeByColumns(dataFrame).toString();
    }

    /**
     * Serialize a data frame into a Json string with the data organized by rows
     * @param dataFrame the data frame to serialize to Json
     * @param includeSchema embed the json representation of the schema of this data frame
     * @return Json string containing the data in the data frame organized by column and (optionally) the data frame
     *         schema.
     */
    public String toJsonStringByColumns(DataFrame dataFrame, boolean includeSchema)
    {
        ObjectNode dfAsJson = new ObjectMapper().createObjectNode();

        dfAsJson.put("name", dataFrame.getName());

        if (includeSchema)
        {
            dfAsJson.set("schema", this.toJsonNodeByRows(dataFrame.schema()));
        }

        dfAsJson.set("data", this.toJsonNodeByColumns(dataFrame));

        return dfAsJson.toString();
    }

    private JsonNode toJsonNodeByRows(DataFrame dataFrame)
    {
        ArrayNode dfByRows = new ObjectMapper().createArrayNode();

        ImmutableList<DfColumn> columns = dataFrame.getColumns();

        dataFrame.forEach(
                row ->
                {
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
            case INT ->  ((DfIntColumn) column).asIntIterable().forEach(values::add);
            case FLOAT ->  ((DfFloatColumn) column).asFloatIterable().forEach(values::add);
            case DOUBLE ->  ((DfDoubleColumn) column).asDoubleIterable().forEach(values::add);
            case DECIMAL ->  ((DfDecimalColumn) column).toList().forEach(values::add);
            case STRING ->  ((DfStringColumn) column).toList().forEach(values::add);
            case BOOLEAN ->  ((DfBooleanColumn) column).toBooleanList().forEach(values::add);
            case DATE ->  ((DfDateColumn) column).toList().forEach(each -> values.add(each.toString()));
            case DATE_TIME ->  ((DfDateTimeColumn) column).toList().forEach(each -> values.add(each.toString()));
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
