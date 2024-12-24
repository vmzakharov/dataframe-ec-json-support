# dataframe-ec-json-support

## Overview

This library adds support for reading and writing [dataframe-ec](https://github.com/vmzakharov/dataframe-ec) data frames in JSON data format. 

Currently, it supports reading and writing dataframe-ec data frames as JSON strings.

## Where to Get It

Get the latest release of `dataframe-ec-json-support` here:

```xml
<dependency>
  <groupId>io.github.vmzakharov</groupId>
  <artifactId>dataframe-ec-json-support</artifactId>
  <version>0.0.3</version>
</dependency>
```

## JsonDataSet class

The main class for dealing with JSON serialization is `JsonDataSet`. It supports the following options for JSON objects: 

* Object structure: just the data frame data or data and metadata
* Metadata (if specified):
  * Data frame name
  * Data frame schema. NOTE: if the schema is not embedded in the JSON object, the schema needs to be specified in the JsonDataSet instance.
* Data organization: by rows or by columns   

## Supported Types

The following data frame column types are supported for serializing data frames to/from JSON:

* STRING
* LONG
* DOUBLE
* INT
* FLOAT
* DATE
* DATE_TIME
* DECIMAL
* BOOLEAN

Reading and writing `null` values is supported for all the supported types.

## Examples

### Serializing a Data Frame to a String

#### Sample Data
For the serialization examples we will be using this data frame
```java
dataFrame = new DataFrame("df")
        .addStringColumn("foo").addLongColumn("bar").addDoubleColumn("baz")
        .addRow("Alice", 10L, 123.45)
        .addRow("Bob", 12L, 222.33)
        .addRow("Carl", 11L, 323.45)
        .addRow("Diane", 14L, 456.78)
        ;
```

#### Data Only, Organized by Rows
```java
JsonDataSet dfToJson = new JsonDataSet("json")
    .dataByRows(true)
    .dataOnly(true);

String jsonString = dfToJson.toJsonString(this.dataFrame);
```

Result: an array of JSON objects, each object representing a single row of the dataframe

```json
[
  {"foo":"Alice","bar":10,"baz":123.45}, 
  {"foo":"Bob","bar":12,"baz":222.33}, 
  {"foo":"Carl","bar":11,"baz":323.45}, 
  {"foo":"Diane","bar":14,"baz":456.78}
]
```

#### Data Only, Organized by Columns
```java
JsonDataSet dfToJson = new JsonDataSet("json")
    .dataOnly(true)
    .dataByRows(false);

String jsonString = dfToJson.toJsonString(this.dataFrame);
```

Result: an array of JSON objects, each object representing a column of the dataframe

```json
[
  {"column":"foo","values":["Alice","Bob","Carl","Diane"]}, 
  {"column":"bar","values":[10,12,11,14]}, 
  {"column":"baz","values":[123.45,222.33,323.45,456.78]}
]
```

#### Data and Metadata (With Schema), by Rows
```java
JsonDataSet dfToJson = new JsonDataSet("json")
      .dataByRows(true)
      .schemaIncluded(true);

String jsonString = dfToJson.toJsonString(this.dataFrame);
```

Result: an object representing the data frame with attributes for the data frame name, data frame schema, and data frame data, where the data is stored as an array of JSON objects, each object representing a single row of the dataframe

```json
{
  "name":"df",
  "schema":[
    {"Name":"foo","Type":"STRING","Stored":"Y","Expression":""}, 
    {"Name":"bar","Type":"LONG","Stored":"Y","Expression":""}, 
    {"Name":"baz","Type":"DOUBLE","Stored":"Y","Expression":""}
  ],
  "data":[
    {"foo":"Alice","bar":10,"baz":123.45}, 
    {"foo":"Bob","bar":12,"baz":222.33}, 
    {"foo":"Carl","bar":11,"baz":323.45}, 
    {"foo":"Diane","bar":14,"baz":456.78}
  ]
}
```

#### Data and Metadata (Without Schema), by Columns
```java
JsonDataSet dfToJson = new JsonDataSet("json")
      .dataByRows(false)
      .schemaIncluded(false);

String jsonString = dfToJson.toJsonString(this.dataFrame);
```

Result: an object representing the data frame with attributes for the data frame name, data frame schema, and data frame data, where the data is stored as an array of JSON objects, each object representing a single row of the dataframe

```json
{
  "name":"df", 
  "data":[
    {"column":"foo","values":["Alice","Bob","Carl","Diane"]}, 
    {"column":"bar","values":[10,12,11,14]}, 
    {"column":"baz","values":[123.45,222.33,323.45,456.78]}
  ]
}
```

### Deserializing a Data Frame from a String

#### Sample Data

We assume that the JSON string for each example is stored in a variable `jsonString`. 

#### Data Only, Organized by Rows

```json
[
  {"foo":"Alice","bar":10,"baz":123.45}, 
  {"foo":"Bob","bar":12,"baz":222.33}, 
  {"foo":"Carl","bar":11,"baz":323.45}, 
  {"foo":"Diane","bar":14,"baz":456.78}
]
```

Since the schema is not stored in the JSON object, it needs to be provided to the instance of `JsonDataSet` separately.

```java
CsvSchema schema = new CsvSchema()
    .addColumn("foo", STRING)
    .addColumn("bar", INT)
    .addColumn("baz", FLOAT)
    ;

JsonDataSet dataSet = new JsonDataSet("data set", schema)
    .dataOnly(true)
    .dataByRows(true);

DataFrame dataFrame = dataSet.fromJsonString(jsonString);
```

#### Data and Metadata, Without Embedded Schema, Organized by Rows
```json
{
  "name":"frame of data", 
  "data":[
    {"foo":"Alice","bar":10,"baz":123.45}, 
    {"foo":"Bob","bar":12,"baz":222.33}, 
    {"foo":"Carl","bar":11,"baz":323.45}, 
    {"foo":"Diane","bar":14,"baz":456.78}
  ]
}
```

Since the schema is not stored in the JSON object, it needs to be provided to the instance of `JsonDataSet` separately.

```java
CsvSchema schema = new CsvSchema()
      .addColumn("foo", STRING)
      .addColumn("bar", INT)
      .addColumn("baz", FLOAT)
      ;

JsonDataSet dataSet = new JsonDataSet("data set", schema)
      .dataOnly(false)
      .dataByRows(true);

DataFrame dataFrame = dataSet.fromJsonString(jsonString);
```

#### Data and Metadata, With Embedded Schema, Organized by Columns
```json
{
  "name":"df", 
  "schema":[
    {"Name":"foo","Type":"STRING","Stored":"Y","Expression":""}, 
    {"Name":"bar","Type":"LONG","Stored":"Y","Expression":""}, 
    {"Name":"baz","Type":"DOUBLE","Stored":"Y","Expression":""}
  ], 
  "data":[
    {"column":"foo","values":["Alice","Bob","Carl","Diane"]}, 
    {"column":"bar","values":[10,12,11,14]}, 
    {"column":"baz","values":[123.45,222.33,323.45,456.78]}
  ]
}
```

Since the schema **is** stored in the JSON object, it does not need to be provided to the instance of `JsonDataSet` separately.

```java
JsonDataSet dataSet = new JsonDataSet("data set")
    .dataOnly(false)
    .dataByRows(false);

DataFrame dataFrame = dataSet.fromJsonString(jsonString);
```
