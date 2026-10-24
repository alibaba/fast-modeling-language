/*
 * Copyright 2022 Alibaba Group Holding Ltd.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.aliyun.fastmodel.transform.mc.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.transform.api.client.CodeGenerator;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.Constraint;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.OutlineConstraintType;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.property.StringProperty;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorModelRequest;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlReverseSqlRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlTableResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.client.generator.DefaultCodeGenerator;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.mc.client.property.AcidDataRetainHours;
import com.aliyun.fastmodel.transform.mc.client.property.AcidIncrementalQueryOutOfTimeRangeEnabled;
import com.aliyun.fastmodel.transform.mc.client.property.AcidTransactional;
import com.aliyun.fastmodel.transform.mc.client.property.AcidWriteBucketNum;
import com.aliyun.fastmodel.transform.mc.client.property.AutoPartitionedByClient;
import com.aliyun.fastmodel.transform.mc.client.property.AutoPartitionedByProperty;
import com.aliyun.fastmodel.transform.mc.format.MaxComputePropertyKey;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * code generator
 *
 * @author panguanjing
 * @date 2022/6/10
 */
public class MaxComputeCodeGeneratorModelTest {

    @Test
    public void testGeneratorNewTable() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        Table table = getTable("c1", "c1", 1000L);
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .caseSensitive(false)
            .build();
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals(dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining(";\n")), "CREATE TABLE IF NOT EXISTS a\n"
            + "(\n"
            + "   c1 STRING NOT NULL COMMENT 'comment'\n"
            + ")\n"
            + "LIFECYCLE 1;");
    }

    @Test
    public void testGeneratorNewTableWithJson() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        Table table = getTable("c1", "c1", 1000L);
        List<Column> columns = table.getColumns();
        columns.add(
            Column.builder()
                .name("jsonTest")
                .dataType("json")
                .build()
        );
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .caseSensitive(false)
            .build();
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals(dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining(";\n")), "CREATE TABLE IF NOT EXISTS a\n"
            + "(\n"
            + "   c1       STRING NOT NULL COMMENT 'comment',\n"
            + "   jsonTest JSON NOT NULL\n"
            + ")\n"
            + "LIFECYCLE 1;");
    }

    @Test
    public void testGeneratorNewTableWithBefore() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        Table table = getTable("c1", "c1", 864000L);
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .caseSensitive(false)
            .build();
        Table beforeTable = getTable("c1", "c1", 0L);
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .before(beforeTable)
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals(dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining("\n")), "ALTER TABLE a ENABLE LIFECYCLE;\n"
            + "ALTER TABLE a SET LIFECYCLE 10;");
    }

    @Test
    public void testGeneratorNewTableWithRenameColumn() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        Table table = getTable("c1", "c2", 1000L);
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .caseSensitive(false)
            .build();
        Table beforeTable = getTable("c1", "c1", 500L);
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .before(beforeTable)
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals(dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining("\n")), "ALTER TABLE a CHANGE COLUMN c1 c2 STRING;");
    }

    @Test
    public void testGeneratorNewTableWithColumnType() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        Table table = getTable("c1", "c2", 1000L);
        List<Column> columns = Lists.newArrayList();
        Column column = Column.builder()
            .dataType("array<int>")
            .name("c3")
            .build();
        columns.add(column);
        table.setColumns(columns);
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .caseSensitive(false)
            .build();
        Table beforeTable = getTable("c1", "c2", 500L);
        beforeTable.setColumns(columns);
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .before(beforeTable)
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals("", dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining("\n")));
    }

    @Test
    public void testGeneratorNewTableWithColumnTypeStruct() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        Table table = getTable("c1", "c2", 1000L);
        List<Column> columns = Lists.newArrayList();
        Column column = Column.builder()
            .dataType("struct<age:int,name:string>")
            .name("c3")
            .build();
        columns.add(column);
        table.setColumns(columns);
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .caseSensitive(false)
            .build();
        Table beforeTable = getTable("c1", "c2", 500L);
        beforeTable.setColumns(columns);
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .before(beforeTable)
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals("", dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining("\n")));
    }

    @Test
    public void testGeneratorNewTableWithColumnTypeMap() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        Table table = getTable("c1", "c2", 1000L);
        List<Column> columns = Lists.newArrayList();
        Column column = Column.builder()
            .dataType("map<int,string>")
            .name("c3")
            .build();
        columns.add(column);
        table.setColumns(columns);
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .caseSensitive(false)
            .build();
        Table beforeTable = getTable("c1", "c2", 500L);
        beforeTable.setColumns(columns);
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .before(beforeTable)
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals("", dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining("\n")));
    }

    @Test
    public void testGeneratorNewTableWithAddColumn() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        Table table = getTable("c1", "c2", 1000L);
        table.getColumns().add(
            Column.builder()
                .name("c3")
                .dataType("decimal")
                .precision(10)
                .scale(10)
                .build()
        );
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .caseSensitive(false)
            .build();
        Table beforeTable = getTable("c1", "c1", 500L);
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .before(beforeTable)
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals(dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining("\n")), "ALTER TABLE a CHANGE COLUMN c1 c2 STRING;\n"
            + "ALTER TABLE a ADD COLUMNS\n"
            + "(\n"
            + "   c3 DECIMAL(10,10)\n"
            + ");");
    }

    @Test
    public void testGeneratorNewTableWithDropColumn() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        Table table = getTable("c1", "c2", 1000L);
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .caseSensitive(false)
            .build();
        Table beforeTable = getTable("c1", "c1", 500L);
        beforeTable.getColumns().add(
            Column.builder()
                .name("c3")
                .dataType("array<string>")
                .build()
        );
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .before(beforeTable)
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals(dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining("\n")), "ALTER TABLE a CHANGE COLUMN c1 c2 STRING;\n"
            + "ALTER TABLE a DROP COLUMN c3;");
    }

    @Test
    public void testGeneratorNewTableWithSchema() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        Table table = Table.builder()
            .schema("sc")
            .name("tb")
            .columns(getColumns("c1", "c1"))
            .build();
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .caseSensitive(false)
            .build();

        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .before(null)
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals(dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining(",\n")), "CREATE TABLE IF NOT EXISTS sc.tb\n"
            + "(\n"
            + "   c1 STRING NOT NULL COMMENT 'comment'\n"
            + ")\n;");
    }

    private Table getTable(String id, String name, long lifeCycleSeconds) {
        List<Column> columns = getColumns(id, name);
        return Table.builder()
            .name("a")
            .columns(columns)
            .lifecycleSeconds(lifeCycleSeconds)
            .build();
    }

    private List<Column> getColumns(String id, String name) {
        return Lists.newArrayList(
            Column.builder()
                .id(id)
                .name(name)
                .dataType("string")
                .comment("comment")
                .build()
        );
    }

    @Test
    public void testConvertToTable() {
        DefaultCodeGenerator defaultCodeGenerator = new DefaultCodeGenerator();
        DdlReverseSqlRequest request = DdlReverseSqlRequest.builder()
            .database("d1")
            .schema("c1")
            .code("create table if not exists a (b string);")
            .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE).build();
        DdlTableResult reverse = defaultCodeGenerator.reverse(request);
        Table table = reverse.getTable();
        assertEquals(table.getDatabase(), "d1");
        assertEquals(table.getSchema(), "c1");
        assertEquals(table.getName(), "a");
        assertEquals(table.getColumns().size(), 1);
    }

    @Test
    public void testConvertToTableIssueLifecycle() {
        DefaultCodeGenerator defaultCodeGenerator = new DefaultCodeGenerator();
        DdlGeneratorModelRequest request1 = DdlGeneratorModelRequest.builder()
            .config(TableConfig.builder().dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE).build())
            .before(Table.builder().name("a")
                .lifecycleSeconds(365000000000L)
                .columns(
                    Lists.newArrayList(Column.builder().name("b").dataType("bigint")
                        .build())
                ).build())
            .after(Table.builder()
                .lifecycleSeconds(null)
                .name("a")
                .columns(Lists.newArrayList(
                    Column.builder()
                        .name("b")
                        .dataType("bigint")
                        .build()
                ))
                .build()
            ).build();
        DdlGeneratorResult reverse = defaultCodeGenerator.generate(request1);
        List<DialectNode> dialectNodes = reverse.getDialectNodes();
        assertEquals(1, dialectNodes.size());
        assertEquals(dialectNodes.get(0).getNode(), "ALTER TABLE a DISABLE LIFECYCLE;");
    }

    @Test
    public void testGeneratorWithTwoPartition() {
        DefaultCodeGenerator defaultCodeGenerator = new DefaultCodeGenerator();
        Column c1 = Column.builder()
            .name("pt")
            .dataType("bigint")
            .partitionKey(true)
            .partitionKeyIndex(0)
            .build();
        Column c2 = Column.builder()
            .name("c2")
            .dataType("bigint")
            .partitionKey(true)
            .partitionKeyIndex(1)
            .build();
        Column c3 = Column.builder()
            .name("c3")
            .dataType("bigint")
            .partitionKey(false)
            .build();
        List<Column> columns = ImmutableList.of(
            c1,
            c2,
            c3
        );
        Table table = Table.builder()
            .name("abc")
            .columns(columns)
            .build();
        DdlGeneratorModelRequest modelRequest = DdlGeneratorModelRequest.builder()
            .after(table)
            .config(TableConfig.builder().dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE).build())
            .build();
        DdlGeneratorResult generate = defaultCodeGenerator.generate(modelRequest);
        assertEquals(generate.getDialectNodes().get(0).getNode(), "CREATE TABLE IF NOT EXISTS abc\n"
            + "(\n"
            + "   c3 BIGINT NOT NULL\n"
            + ")\n"
            + "PARTITIONED BY\n"
            + "(\n"
            + "   pt BIGINT,\n"
            + "   c2 BIGINT\n"
            + ")\n;");
    }

    @Test
    public void testGenerateTransactional() {
        DefaultCodeGenerator defaultCodeGenerator = new DefaultCodeGenerator();
        List<Column> columns = Lists.newArrayList(
            Column.builder()
                .name("c1")
                .dataType("bigint")
                .nullable(false)
                .build()
        );
        AcidTransactional transactional = new AcidTransactional();
        transactional.setValue(true);

        AcidWriteBucketNum writeBucketNum = new AcidWriteBucketNum();
        writeBucketNum.setValue(10);

        AcidDataRetainHours dataRetainHours = new AcidDataRetainHours();
        dataRetainHours.setValueString("10");

        AcidIncrementalQueryOutOfTimeRangeEnabled rangeEnabled = new AcidIncrementalQueryOutOfTimeRangeEnabled();
        rangeEnabled.setValue(true);

        List<BaseClientProperty> properties = Lists.newArrayList(
            transactional,
            writeBucketNum,
            dataRetainHours,
            rangeEnabled
        );
        ArrayList<String> strings = Lists.newArrayList("c1");
        List<Constraint> constraints = Lists.newArrayList(
            Constraint.builder().columns(strings)
                .type(OutlineConstraintType.PRIMARY_KEY)
                .build()
        );
        Table table = Table.builder()
            .name("abc")
            .columns(columns)
            .properties(properties)
            .constraints(constraints)
            .build();
        DdlGeneratorModelRequest modelRequest = DdlGeneratorModelRequest.builder()
            .after(table)
            .config(TableConfig.builder().dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE).build())
            .build();
        DdlGeneratorResult generate = defaultCodeGenerator.generate(modelRequest);
        assertEquals(generate.getDialectNodes().get(0).getNode(), "CREATE TABLE IF NOT EXISTS abc\n"
            + "(\n"
            + "   c1 BIGINT NOT NULL,\n"
            + "   PRIMARY KEY(c1)\n"
            + ")\n"
            + "TBLPROPERTIES ('transactional'='true','write.bucket.num'='10','acid.data.retain.hours'='10','acid.incremental.query.out.of.time"
            + ".range.enabled'='true');");

    }

    @Test
    public void testGenerateTransactionalStringProperty() {
        DefaultCodeGenerator defaultCodeGenerator = new DefaultCodeGenerator();
        List<Column> columns = Lists.newArrayList(
            Column.builder()
                .name("c1")
                .dataType("bigint")
                .nullable(false)
                .build()
        );
        StringProperty transactional = new StringProperty();
        transactional.setKey(MaxComputePropertyKey.TRANSACTIONAL.getValue());
        transactional.setValue("true");

        StringProperty writeBucketNum = new StringProperty();
        writeBucketNum.setKey(MaxComputePropertyKey.WRITE_BUCKET_NUM.getValue());
        writeBucketNum.setValue("10");

        StringProperty dataRetainHours = new StringProperty();
        dataRetainHours.setKey(MaxComputePropertyKey.ACID_DATA_RETAIN_HOURS.getValue());
        dataRetainHours.setValueString("10");

        StringProperty rangeEnabled = new StringProperty();
        rangeEnabled.setKey(MaxComputePropertyKey.ACID_INCREMENTAL_QUERY_OUT_OF_TIME_RANGE_ENABLED.getValue());
        rangeEnabled.setValueString("true");

        List<BaseClientProperty> properties = Lists.newArrayList(
            transactional,
            writeBucketNum,
            dataRetainHours,
            rangeEnabled
        );
        Constraint constraint = Constraint.builder().name(null)
            .columns(Lists.newArrayList("c1"))
            .type(OutlineConstraintType.PRIMARY_KEY)
            .build();
        List<Constraint> constraints = Lists.newArrayList(
            constraint
        );
        Table table = Table.builder()
            .name("abc")
            .columns(columns)
            .properties(properties)
            .constraints(constraints)
            .build();
        DdlGeneratorModelRequest modelRequest = DdlGeneratorModelRequest.builder()
            .after(table)
            .config(TableConfig.builder().dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE).build())
            .build();
        DdlGeneratorResult generate = defaultCodeGenerator.generate(modelRequest);
        assertEquals(generate.getDialectNodes().get(0).getNode(), "CREATE TABLE IF NOT EXISTS abc\n"
            + "(\n"
            + "   c1 BIGINT NOT NULL,\n"
            + "   PRIMARY KEY(c1)\n"
            + ")\n"
            + "TBLPROPERTIES ('transactional'='true','write.bucket.num'='10','acid.data.retain.hours'='10','acid.incremental.query.out.of.time"
            + ".range.enabled'='true');");

    }

    @Test
    public void testGeneratorWithKeyWords() {
        DefaultCodeGenerator defaultCodeGenerator = new DefaultCodeGenerator();
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .caseSensitive(false)
            .build();
        List<Column> columns = Lists.newArrayList(
            Column.builder().name("double").dataType("bigint").build(),
            Column.builder().name("bigint").dataType("bigint").build()
        );
        Table table = Table.builder()
            .name("abc")
            .columns(columns)
            .build();
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult reverse = defaultCodeGenerator.generate(request);
        List<DialectNode> dialectNodes = reverse.getDialectNodes();
        assertEquals(dialectNodes.get(0).getNode(), "CREATE TABLE IF NOT EXISTS abc\n"
            + "(\n"
            + "   `double` BIGINT NOT NULL,\n"
            + "   `bigint` BIGINT NOT NULL\n"
            + ")\n"
            + ";");
    }

    @Test
    public void testGeneratorPrimaryKey() {
        DefaultCodeGenerator defaultCodeGenerator = new DefaultCodeGenerator();
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .caseSensitive(false)
            .build();
        List<Column> columns = Lists.newArrayList(
            Column.builder().name("double").primaryKey(true).dataType("bigint").build(),
            Column.builder().name("bigint").dataType("bigint").nullable(true).build()
        );
        StringProperty stringProperty = new StringProperty();
        stringProperty.setKey(MaxComputePropertyKey.TRANSACTIONAL.getValue());
        stringProperty.setValueString("true");
        List<BaseClientProperty> properties = Lists.newArrayList(
            stringProperty
        );
        Table table = Table.builder()
            .name("abc")
            .columns(columns)
            .properties(properties)
            .lifecycleSeconds(TimeUnit.DAYS.toSeconds(1L))
            .build();
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult reverse = defaultCodeGenerator.generate(request);
        List<DialectNode> dialectNodes = reverse.getDialectNodes();
        assertEquals(dialectNodes.get(0).getNode(), "CREATE TABLE IF NOT EXISTS abc\n"
            + "(\n"
            + "   `double` BIGINT NOT NULL PRIMARY KEY,\n"
            + "   `bigint` BIGINT\n"
            + ")\n"
            + "TBLPROPERTIES ('transactional'='true')\n"
            + "LIFECYCLE 1;");
    }

    @Test
    public void testAcidBeforeTable() {
        DefaultCodeGenerator defaultCodeGenerator = new DefaultCodeGenerator();
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .caseSensitive(false)
            .build();
        List<Column> columns = Lists.newArrayList(
            Column.builder().name("double").primaryKey(true).dataType("bigint").build(),
            Column.builder().name("bigint").dataType("bigint").nullable(true).build()
        );
        StringProperty stringProperty = new StringProperty();
        stringProperty.setKey(MaxComputePropertyKey.TRANSACTIONAL.getValue());
        stringProperty.setValueString("true");
        List<BaseClientProperty> properties = Lists.newArrayList(
            stringProperty
        );
        Table table = Table.builder()
            .name("abc")
            .columns(columns)
            .properties(properties)
            .lifecycleSeconds(TimeUnit.DAYS.toSeconds(1L))
            .build();

        StringProperty stringProperty2 = new StringProperty();
        stringProperty2.setKey(MaxComputePropertyKey.WRITE_BUCKET_NUM.getValue());
        stringProperty2.setValueString("10");
        List<BaseClientProperty> properties1 = Lists.newArrayList(
            stringProperty,
            stringProperty2
        );
        Table after = Table.builder()
            .name("abc")
            .columns(columns)
            .properties(properties1)
            .lifecycleSeconds(TimeUnit.DAYS.toSeconds(1L))
            .build();

        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .before(table)
            .after(after)
            .config(config)
            .build();
        DdlGeneratorResult reverse = defaultCodeGenerator.generate(request);
        List<DialectNode> dialectNodes = reverse.getDialectNodes();
        assertFalse(dialectNodes.get(0).isExecutable());
    }

    @Test
    public void testGeneratorThreeTile() {
        DefaultCodeGenerator defaultCodeGenerator = new DefaultCodeGenerator();
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .caseSensitive(false)
            .build();
        List<Column> columns = Lists.newArrayList();
        Column c1 = Column.builder()
            .name("c1")
            .dataType("bigint")
            .build();
        columns.add(c1);
        DdlReverseSqlRequest request =
            DdlReverseSqlRequest.builder()
                .code("create table autotest.t1 (c1 string)")
                .database("autotest")
                .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
                .schema(null)
                .build();
        DdlTableResult generate = defaultCodeGenerator.reverse(request);
        Table table = generate.getTable();
        DdlGeneratorModelRequest sqlResult = DdlGeneratorModelRequest.builder()
            .before(null)
            .after(table)
            .config(config).build();
        DdlGeneratorResult generate1 = defaultCodeGenerator.generate(sqlResult);
        assertEquals("CREATE TABLE autotest.t1\n" +
            "(\n" +
            "   c1 STRING\n" +
            ")\n" +
            ";", generate1.getDialectNodes().get(0).getNode());
    }

    @Test
    public void testGeneratorThreeTile2() {
        DefaultCodeGenerator defaultCodeGenerator = new DefaultCodeGenerator();
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .caseSensitive(false)
            .build();
        List<Column> columns = Lists.newArrayList();
        Column c1 = Column.builder()
            .name("c1")
            .dataType("bigint")
            .build();
        columns.add(c1);
        DdlReverseSqlRequest request =
            DdlReverseSqlRequest.builder()
                .code("create table autotest.s1.t1 (c1 string)")
                .database("autotest")
                .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
                .schema(null)
                .build();
        DdlTableResult generate = defaultCodeGenerator.reverse(request);
        Table table = generate.getTable();
        DdlGeneratorModelRequest sqlResult = DdlGeneratorModelRequest.builder()
            .before(null)
            .after(table)
            .config(config).build();
        DdlGeneratorResult generate1 = defaultCodeGenerator.generate(sqlResult);
        assertEquals("CREATE TABLE autotest.s1.t1\n" +
            "(\n" +
            "   c1 STRING\n" +
            ")\n" +
            ";", generate1.getDialectNodes().get(0).getNode());
    }

    @Test
    public void testGeneratorWithNtz() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        Table table = Table.builder()
            .name("a")
            .columns(Lists.newArrayList(
                Column.builder().name("c1").dataType("timestamp_ntz").comment("comment").build()
            ))
            .lifecycleSeconds(TimeUnit.DAYS.toSeconds(1L))
            .build();
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .caseSensitive(false)
            .build();
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals(dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining(";\n")), "CREATE TABLE IF NOT EXISTS a\n"
            + "(\n"
            + "   c1 TIMESTAMP_NTZ NOT NULL COMMENT 'comment'\n"
            + ")\n"
            + "LIFECYCLE 1;");
    }

    @Test
    public void testGeneratorWithSpecialChar() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        Table table = Table.builder()
            .name("a")
            .columns(Lists.newArrayList(
                Column.builder().name("p1:test").dataType("string").comment("comment").build()
            ))
            .lifecycleSeconds(TimeUnit.DAYS.toSeconds(1L))
            .build();
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .caseSensitive(false)
            .build();
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals("CREATE TABLE IF NOT EXISTS a\n"
            + "(\n"
            + "   `p1:test` STRING NOT NULL COMMENT 'comment'\n"
            + ")\n"
            + "LIFECYCLE 1;", dialectNodes.get(0).getNode());
    }

    @Test
    public void testGeneratorWithAutoPartitionedBy() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        AutoPartitionedByProperty aTrue = new AutoPartitionedByProperty();
        AutoPartitionedByClient autoPartitionedByClient = new AutoPartitionedByClient();
        autoPartitionedByClient.setExpression("TRUNC_TIME(_partitiontime, 'hour')");
        autoPartitionedByClient.setAlias("sale_time");
        aTrue.setValue(autoPartitionedByClient);
        Table table = Table.builder()
            .name("ingestion_sale_detail_hourly")
            .columns(Lists.newArrayList(
                Column.builder().name("shop_name").dataType("string").build(),
                Column.builder().name("total_price").dataType("double").build(),
                Column.builder().name("_partitiontime").dataType("timestamp").build()
            ))
            .properties(Lists.newArrayList(
                aTrue
            ))
            .build();
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .caseSensitive(false)
            .build();
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals("CREATE TABLE IF NOT EXISTS ingestion_sale_detail_hourly\n"
                + "(\n"
                + "   shop_name        STRING NOT NULL,\n"
                + "   total_price      DOUBLE NOT NULL,\n"
                + "   `_partitiontime` TIMESTAMP NOT NULL\n"
                + ")\n"
                + "AUTO PARTITIONED BY\n"
                + "(\n"
                + "   TRUNC_TIME(`_partitiontime`, 'hour') AS sale_time\n"
                + ")\n"
                + ";",
            dialectNodes.get(0).getNode());
    }
}