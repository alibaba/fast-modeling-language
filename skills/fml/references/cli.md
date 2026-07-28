# FML CLI reference

## Commands

All examples assume:

```bash
FML_CLI_JAR="$(find fastmodel-agent-cli/target -name 'fastmodel-agent-cli-*-executable.jar' \
  -type f -print -quit)"
test -n "$FML_CLI_JAR"
```

Validate:

```bash
java -jar "$FML_CLI_JAR" validate model.fml
java -jar "$FML_CLI_JAR" validate --dialect mysql schema.sql
java -jar "$FML_CLI_JAR" validate --text "create dim table dim_shop (shop_id bigint);"
java -jar "$FML_CLI_JAR" validate -
```

Format:

```bash
java -jar "$FML_CLI_JAR" format model.fml
```

Inspect the parsed tree:

```bash
java -jar "$FML_CLI_JAR" inspect model.fml
```

Transform:

```bash
java -jar "$FML_CLI_JAR" transform --dialect hive model.fml
java -jar "$FML_CLI_JAR" transform --dialect mysql model.fml
java -jar "$FML_CLI_JAR" transform --dialect hologres@3.0 model.fml
java -jar "$FML_CLI_JAR" transform --dialect plantuml model.fml
```

Discover the installed contract:

```bash
java -jar "$FML_CLI_JAR" capabilities
```

The returned `dialects` array is generated from Java `ServiceLoader` metadata in the packaged JAR. Each item reports:

- `name`: value accepted by `--dialect`
- `transform`: supports FML-to-dialect output
- `validate`: supports parsing native dialect input

The current package discovers ADB MySQL, ADB PostgreSQL, ClickHouse, Doris, Flink, FML, graph, Hive, Hologres (default/2.0/3.0), MySQL, OceanBase MySQL, Oracle, PlantUML, PostgreSQL, Spark, SQLite, StarRocks, and Zen implementations. ClickHouse is native-input only; graph and PlantUML are output-only. PostgreSQL native validation is not advertised until its reverse parser reliably returns a statement.

## Success response shapes

Validation:

```json
{
  "ok": true,
  "statementCount": 1,
  "dialect": "fml",
  "statementTypes": ["CreateDimTable"],
  "diagnostics": []
}
```

Formatting and transformation:

```json
{
  "ok": true,
  "dialect": "mysql",
  "output": "CREATE TABLE ..."
}
```

Inspection:

```json
{
  "ok": true,
  "statementCount": 1,
  "statements": [
    {
      "type": "CreateDimTable",
      "children": []
    }
  ]
}
```

## Error response

```json
{
  "ok": false,
  "diagnostics": [
    {
      "code": "FML_PARSE_ERROR",
      "message": "parser message",
      "line": 1,
      "column": 12
    }
  ]
}
```

Parse stdout as JSON. Use the process exit code for control flow and the diagnostic fields for repair.
