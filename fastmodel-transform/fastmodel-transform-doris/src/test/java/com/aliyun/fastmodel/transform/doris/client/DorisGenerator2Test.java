package com.aliyun.fastmodel.transform.doris.client;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.expr.enums.DateTimeEnum;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.client.CodeGenerator;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.Constraint;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.property.StringProperty;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorModelRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.client.generator.DefaultCodeGenerator;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.extension.client.constraint.ClientConstraintType;
import com.aliyun.fastmodel.transform.api.extension.client.constraint.DistributeClientConstraint;
import com.aliyun.fastmodel.transform.api.extension.client.property.column.AggrColumnProperty;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.MultiRangePartitionProperty;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.ReplicationNum;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.SingleRangePartitionProperty;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.TablePartitionRaw;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.ArrayClientPartitionKey;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.LessThanClientPartitionKey;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.MultiRangeClientPartition;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.PartitionClientValue;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.SingleRangeClientPartition;
import com.aliyun.fastmodel.transform.api.extension.tree.column.AggregateDesc;
import com.aliyun.fastmodel.transform.doris.parser.DorisLanguageParser;
import com.google.common.collect.Lists;
import org.junit.Test;

import static com.aliyun.fastmodel.core.tree.expr.literal.CurrentTimestamp.CURRENT_TIMESTAMP;
import static com.aliyun.fastmodel.transform.doris.parser.tree.DorisDataTypeName.DATETIME;
import static com.aliyun.fastmodel.transform.doris.parser.tree.DorisDataTypeName.FLOAT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * ddl generator test
 *
 * @author panguanjing
 * @date 2023/9/17
 */
public class DorisGenerator2Test {
    CodeGenerator codeGenerator = new DefaultCodeGenerator();

    DorisLanguageParser dorisLanguageParser = new DorisLanguageParser();

    /**
     * 通过string property传入
     */
    @Test
    public void testGeneratorProperties1() {
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder()
            .dataType("decimal")
            .precision(10)
            .scale(4)
            .name("c1")
            .nullable(true)
            .build());

        List<BaseClientProperty> properties = Lists.newArrayList();
        StringProperty stringProperty = new StringProperty();
        stringProperty.setKey("replication_num");
        stringProperty.setValueString("3");
        properties.add(stringProperty);
        Table after = Table.builder()
            .database("autotest")
            .name("abc")
            .columns(columns)
            .comment("comment")
            .properties(properties)
            .build();
        request.setAfter(after);
        DdlGeneratorResult generate = getDdlGeneratorResult(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes().stream()
            .filter(DialectNode::getExecutable)
            .collect(Collectors.toList());
        assertEquals("CREATE TABLE IF NOT EXISTS autotest.abc\n"
            + "(\n"
            + "   c1 DECIMAL(10,4) NULL\n"
            + ")\n"
            + "COMMENT \"comment\"\n"
            + "PROPERTIES (\"replication_num\"=\"3\");", dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining("\n")));
    }

    /**
     * 通过固定的property传入
     */
    @Test
    public void testGeneratorProperties2() {
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder()
            .dataType("decimal")
            .precision(10)
            .scale(4)
            .name("c1")
            .nullable(true)
            .build());

        List<BaseClientProperty> properties = Lists.newArrayList();
        ReplicationNum stringProperty = new ReplicationNum();
        stringProperty.setValueString("3");
        properties.add(stringProperty);
        Table after = Table.builder()
            .name("abc")
            .columns(columns)
            .comment("comment")
            .properties(properties)
            .build();
        request.setAfter(after);
        DdlGeneratorResult generate = getDdlGeneratorResult(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes().stream()
            .filter(DialectNode::getExecutable)
            .collect(Collectors.toList());
        assertEquals("CREATE TABLE IF NOT EXISTS abc\n"
            + "(\n"
            + "   c1 DECIMAL(10,4) NULL\n"
            + ")\n"
            + "COMMENT \"comment\"\n"
            + "PROPERTIES (\"replication_num\"=\"3\");", dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining("\n")));
    }

    @Test
    public void testGeneratorSupportDecimal() {
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder()
            .dataType("decimal")
            .precision(10)
            .scale(4)
            .name("c1")
            .nullable(true)
            .build());

        Table after = Table.builder()
            .name("abc")
            .columns(columns)
            .comment("comment")
            .build();
        request.setAfter(after);
        DdlGeneratorResult generate = getDdlGeneratorResult(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes().stream()
            .filter(DialectNode::getExecutable)
            .collect(Collectors.toList());
        assertEquals("CREATE TABLE IF NOT EXISTS abc\n"
            + "(\n"
            + "   c1 DECIMAL(10,4) NULL\n"
            + ")\n"
            + "COMMENT \"comment\";", dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining("\n")));
    }

    @Test
    public void testGeneratorSupportArrayInt() {
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder()
            .dataType("array<int>")
            .name("c1")
            .nullable(true)
            .build());

        Table after = Table.builder()
            .name("abc")
            .columns(columns)
            .comment("comment")
            .build();
        request.setAfter(after);
        DdlGeneratorResult generate = getDdlGeneratorResult(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes().stream()
            .filter(DialectNode::getExecutable)
            .collect(Collectors.toList());
        assertEquals("CREATE TABLE IF NOT EXISTS abc\n"
            + "(\n"
            + "   c1 ARRAY<INT> NULL\n"
            + ")\n"
            + "COMMENT \"comment\";", dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining("\n")));
    }

    @Test
    public void testGeneratorSupportPartitionRaw() {
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder()
            .dataType("int")
            .name("c1")
            .nullable(true)
            .build());
        List<BaseClientProperty> properties = Lists.newArrayList();
        TablePartitionRaw e = new TablePartitionRaw();
        e.setValueString("PARTITION BY RANGE (pay_dt) (\n"
            + "  PARTITION p1 VALUES LESS THAN (\"20210102\"),\n"
            + "  PARTITION p2 VALUES LESS THAN (\"20210103\"),\n"
            + "  PARTITION p3 VALUES LESS THAN MAXVALUE\n"
            + ")");
        properties.add(e);

        DistributeClientConstraint starRocksDistributeConstraint = new DistributeClientConstraint();
        starRocksDistributeConstraint.setColumns(Lists.newArrayList("c1"));
        List<Constraint> list = Lists.newArrayList(starRocksDistributeConstraint);

        Table after = Table.builder()
            .name("abc")
            .columns(columns)
            .comment("comment")
            .properties(properties)
            .constraints(list)
            .build();
        request.setAfter(after);
        DdlGeneratorResult generate = getDdlGeneratorResult(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes().stream()
            .filter(DialectNode::getExecutable)
            .collect(Collectors.toList());
        assertEquals("CREATE TABLE IF NOT EXISTS abc\n"
            + "(\n"
            + "   c1 INT NULL\n"
            + ")\n"
            + "COMMENT \"comment\"\n"
            + "PARTITION BY RANGE (pay_dt) (\n"
            + "  PARTITION p1 VALUES LESS THAN (\"20210102\"),\n"
            + "  PARTITION p2 VALUES LESS THAN (\"20210103\"),\n"
            + "  PARTITION p3 VALUES LESS THAN MAXVALUE\n"
            + ")\n"
            + "DISTRIBUTED BY HASH(c1);", dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining("\n")));
    }

    @Test
    public void testGeneratorSupportDistribute() {
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder()
            .dataType("int")
            .name("c1")
            .nullable(true)
            .build());

        DistributeClientConstraint starRocksDistributeConstraint = new DistributeClientConstraint();
        starRocksDistributeConstraint.setColumns(Lists.newArrayList("c1"));
        starRocksDistributeConstraint.setBucket(1);
        List<Constraint> list = Lists.newArrayList(starRocksDistributeConstraint);

        Table after = Table.builder()
            .name("abc")
            .columns(columns)
            .comment("comment")
            .constraints(list)
            .build();
        request.setAfter(after);
        DdlGeneratorResult generate = getDdlGeneratorResult(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes().stream()
            .filter(DialectNode::getExecutable)
            .collect(Collectors.toList());
        assertEquals("CREATE TABLE IF NOT EXISTS abc\n"
            + "(\n"
            + "   c1 INT NULL\n"
            + ")\n"
            + "COMMENT \"comment\"\n"
            + "DISTRIBUTED BY HASH(c1) BUCKETS 1;", dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining("\n")));
    }

    @Test
    public void testGeneratorSingleRangePartitionProperty() {
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder()
            .dataType("int")
            .name("c1")
            .nullable(true)
            .build());

        columns.add(Column.builder()
            .dataType("int")
            .name("c2")
            .partitionKeyIndex(0)
            .partitionKey(true)
            .comment("comment")
            .build());
        List<BaseClientProperty> properties = Lists.newArrayList();
        //first
        SingleRangePartitionProperty e = new SingleRangePartitionProperty();
        SingleRangeClientPartition value = new SingleRangeClientPartition();
        value.setName("a1");
        value.setIfNotExists(true);
        List<List<PartitionClientValue>> partitionValues = Lists.newArrayList();
        PartitionClientValue p1 = PartitionClientValue.builder()
            .value("2021-01-01")
            .build();
        partitionValues.add(Lists.newArrayList(p1));
        PartitionClientValue p2 = PartitionClientValue.builder()
            .value("2022-01-02")
            .build();
        PartitionClientValue p3 = PartitionClientValue.builder()
            .maxValue(true)
            .build();
        ;
        partitionValues.add(Lists.newArrayList(p2, p3));
        ArrayClientPartitionKey partitionKey = ArrayClientPartitionKey.builder()
            .partitionValue(partitionValues)
            .build();
        value.setPartitionKey(partitionKey);
        e.setValue(value);
        properties.add(e);

        //second
        //first
        e = new SingleRangePartitionProperty();
        value = new SingleRangeClientPartition();
        value.setName("a2");
        value.setIfNotExists(false);
        partitionValues = Lists.newArrayList();
        p1 = PartitionClientValue.builder()
            .value("2021-01-01")
            .build();
        partitionValues.add(Lists.newArrayList(p1));
        p2 = PartitionClientValue.builder()
            .value("2022-01-02")
            .build();
        p3 = PartitionClientValue.builder()
            .maxValue(true)
            .build();
        partitionValues.add(Lists.newArrayList(p2, p3));
        partitionKey = ArrayClientPartitionKey.builder()
            .partitionValue(partitionValues)
            .build();
        value.setPartitionKey(partitionKey);
        e.setValue(value);
        properties.add(e);
        Table after = Table.builder()
            .name("abc")
            .columns(columns)
            .comment("comment")
            .properties(properties)
            .build();
        request.setAfter(after);
        DdlGeneratorResult generate = getDdlGeneratorResult(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals("CREATE TABLE IF NOT EXISTS abc\n"
            + "(\n"
            + "   c1 INT NULL,\n"
            + "   c2 INT NOT NULL COMMENT \"comment\"\n"
            + ")\n"
            + "COMMENT \"comment\"\n"
            + "PARTITION BY RANGE (c2)\n"
            + "(\n"
            + "   PARTITION IF NOT EXISTS a1 VALUES [(\"2021-01-01\"),(\"2022-01-02\",MAXVALUE)),\n"
            + "   PARTITION a2 VALUES [(\"2021-01-01\"),(\"2022-01-02\",MAXVALUE))\n"
            + ");", dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining("\n")));
    }

    @Test
    public void testGeneratorSingleRangePartitionPropertyWithLessThan() {
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder()
            .dataType("int")
            .name("c1")
            .nullable(true)
            .build());

        columns.add(Column.builder()
            .dataType("int")
            .name("c2")
            .partitionKeyIndex(0)
            .partitionKey(true)
            .comment("comment")
            .build());
        List<BaseClientProperty> properties = Lists.newArrayList();
        SingleRangePartitionProperty e = new SingleRangePartitionProperty();
        SingleRangeClientPartition value = new SingleRangeClientPartition();
        value.setName("a1");
        value.setIfNotExists(true);
        LessThanClientPartitionKey partitionKey = LessThanClientPartitionKey.builder()
            .maxValue(false)
            .partitionValueList(Lists.newArrayList(PartitionClientValue.builder().value("2020-01-01").build(),
                PartitionClientValue.builder().value("2021-02-03").build()))
            .build();
        value.setPartitionKey(partitionKey);
        e.setValue(value);
        properties.add(e);
        Table after = Table.builder()
            .name("abc")
            .columns(columns)
            .comment("comment")
            .properties(properties)
            .build();
        request.setAfter(after);
        DdlGeneratorResult generate = getDdlGeneratorResult(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals("CREATE TABLE IF NOT EXISTS abc\n"
            + "(\n"
            + "   c1 INT NULL,\n"
            + "   c2 INT NOT NULL COMMENT \"comment\"\n"
            + ")\n"
            + "COMMENT \"comment\"\n"
            + "PARTITION BY RANGE (c2)\n"
            + "(\n"
            + "   PARTITION IF NOT EXISTS a1 VALUES LESS THAN (\"2020-01-01\",\"2021-02-03\")\n"
            + ");", dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining("\n")));
    }

    private DdlGeneratorResult getDdlGeneratorResult(DdlGeneratorModelRequest request) {
        request.setConfig(TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_DORIS)
            .build());
        return codeGenerator.generate(request);
    }

    @Test
    public void testGeneratorMultiRange() {
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        List<Column> columns = Lists.newArrayList();
        List<BaseClientProperty> columnProp = Lists.newArrayList();
        AggrColumnProperty aggrColumnProperty = new AggrColumnProperty();
        aggrColumnProperty.setValueString(AggregateDesc.MIN.name());
        columnProp.add(aggrColumnProperty);

        columns.add(Column.builder()
            .dataType("int")
            .name("c1")
            .nullable(true)
            .properties(columnProp)
            .build());

        columns.add(Column.builder()
            .dataType("int")
            .name("c2")
            .partitionKeyIndex(0)
            .partitionKey(true)
            .comment("comment")
            .build());
        List<BaseClientProperty> properties = Lists.newArrayList();
        SingleRangePartitionProperty e = new SingleRangePartitionProperty();
        SingleRangeClientPartition value = new SingleRangeClientPartition();
        value.setName("a1");
        value.setIfNotExists(true);
        ArrayList<PartitionClientValue> strings = Lists.newArrayList(PartitionClientValue.builder().value("2001-01-01").build(),
            PartitionClientValue.builder().value("2002-01-01").build());
        LessThanClientPartitionKey partitionKeyValue = LessThanClientPartitionKey.builder()
            .partitionValueList(strings)
            .build();
        value.setPartitionKey(partitionKeyValue);
        e.setValue(value);
        properties.add(e);
        Table after = Table.builder()
            .name("abc")
            .columns(columns)
            .comment("comment")
            .properties(properties)
            .build();
        request.setAfter(after);
        DdlGeneratorResult generate = getDdlGeneratorResult(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals("CREATE TABLE IF NOT EXISTS abc\n"
            + "(\n"
            + "   c1 INT MIN NULL,\n"
            + "   c2 INT NOT NULL COMMENT \"comment\"\n"
            + ")\n"
            + "COMMENT \"comment\"\n"
            + "PARTITION BY RANGE (c2)\n"
            + "(\n"
            + "   PARTITION IF NOT EXISTS a1 VALUES LESS THAN (\"2001-01-01\",\"2002-01-01\")\n"
            + ");", dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining("\n")));
    }

    @Test
    public void testGeneratorMultiRangeInterval() {
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        List<Column> columns = Lists.newArrayList();
        List<BaseClientProperty> columnProp = Lists.newArrayList();
        AggrColumnProperty aggrColumnProperty = new AggrColumnProperty();
        aggrColumnProperty.setValueString(AggregateDesc.MIN.name());
        columnProp.add(aggrColumnProperty);

        columns.add(Column.builder()
            .dataType("int")
            .name("c1")
            .nullable(true)
            .properties(columnProp)
            .build());

        columns.add(Column.builder()
            .dataType("int")
            .name("c2")
            .partitionKeyIndex(0)
            .partitionKey(true)
            .comment("comment")
            .build());
        List<BaseClientProperty> properties = Lists.newArrayList();
        MultiRangePartitionProperty e = new MultiRangePartitionProperty();
        MultiRangeClientPartition v = new MultiRangeClientPartition();
        v.setStart("2020-01-01");
        v.setEnd("2023-01-01");
        v.setDateTimeEnum(DateTimeEnum.DAY);
        v.setInterval(1L);
        e.setValue(v);
        properties.add(e);
        Table after = Table.builder()
            .name("abc")
            .columns(columns)
            .comment("comment")
            .properties(properties)
            .build();
        request.setAfter(after);
        DdlGeneratorResult generate = getDdlGeneratorResult(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals("CREATE TABLE IF NOT EXISTS abc\n"
            + "(\n"
            + "   c1 INT MIN NULL,\n"
            + "   c2 INT NOT NULL COMMENT \"comment\"\n"
            + ")\n"
            + "COMMENT \"comment\"\n"
            + "PARTITION BY RANGE (c2)\n"
            + "(\n"
            + "   FROM (\"2020-01-01\") TO (\"2023-01-01\") INTERVAL 1 DAY\n"
            + ");", dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining("\n")));
    }

    @Test
    public void testKeyWords() {
        List<Column> columns = Lists.newArrayList();
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        columns.add(Column.builder()
            .dataType("tinyint")
            .name("add")
            .nullable(false)
            .primaryKey(true)
            .comment("ti_comment")
            .build());
        columns.add(Column.builder()
            .dataType("smallint")
            .name("c2")
            .nullable(false)
            .primaryKey(true)
            .comment("si_comment")
            .build());
        Table after = Table.builder()
            .name("abc")
            .columns(columns)
            .comment("comment")
            .build();
        request.setAfter(after);
        DdlGeneratorResult generate = getDdlGeneratorResult(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals(1, dialectNodes.size());
        assertEquals("CREATE TABLE IF NOT EXISTS abc\n"
            + "(\n"
            + "   `add` TINYINT NOT NULL COMMENT \"ti_comment\",\n"
            + "   c2    SMALLINT NOT NULL COMMENT \"si_comment\"\n"
            + ")\n"
            + "PRIMARY KEY (`add`,c2)\n"
            + "COMMENT \"comment\";", dialectNodes.get(0).getNode());
    }

    @Test
    public void testGeneratorDistribute() {
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder()
            .dataType("decimal")
            .precision(10)
            .scale(4)
            .name("add")
            .nullable(true)
            .primaryKey(true)
            .build());

        List<Constraint> constraints = Lists.newArrayList();
        DistributeClientConstraint distributeConstraint = new DistributeClientConstraint();
        distributeConstraint.setBucket(1);
        distributeConstraint.setColumns(Lists.newArrayList("add"));
        constraints.add(distributeConstraint);
        Table after = Table.builder()
            .name("abc")
            .columns(columns)
            .comment("comment")
            .constraints(constraints)
            .build();
        request.setAfter(after);
        DdlGeneratorResult generate = getDdlGeneratorResult(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes().stream()
            .filter(DialectNode::getExecutable)
            .collect(Collectors.toList());
        assertEquals("CREATE TABLE IF NOT EXISTS abc\n"
            + "(\n"
            + "   `add` DECIMAL(10,4) NULL\n"
            + ")\n"
            + "COMMENT \"comment\"\n"
            + "DISTRIBUTED BY HASH(`add`) BUCKETS 1;", dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining("\n")));
    }

    @Test
    public void testGeneratorDistributeRandom() {
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder()
            .dataType("decimal")
            .precision(10)
            .scale(4)
            .name("add")
            .nullable(true)
            .primaryKey(true)
            .build());

        List<Constraint> constraints = Lists.newArrayList();
        DistributeClientConstraint distributeConstraint = new DistributeClientConstraint();
        distributeConstraint.setRandom(true);
        constraints.add(distributeConstraint);
        Table after = Table.builder()
            .name("abc")
            .columns(columns)
            .comment("comment")
            .constraints(constraints)
            .build();
        request.setAfter(after);
        DdlGeneratorResult generate = getDdlGeneratorResult(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes().stream()
            .filter(DialectNode::getExecutable)
            .collect(Collectors.toList());
        assertEquals("CREATE TABLE IF NOT EXISTS abc\n"
            + "(\n"
            + "   `add` DECIMAL(10,4) NULL\n"
            + ")\n"
            + "COMMENT \"comment\"\n"
            + "DISTRIBUTED BY RANDOM;", dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining("\n")));
    }

    @Test
    public void testGeneratorCommentWithConverter() {
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder()
            .dataType("decimal")
            .precision(10)
            .scale(4)
            .name("add")
            .primaryKey(true)
            .build());

        Table after = Table.builder()
            .name("abc")
            .columns(columns)
            .comment("\"\"comment")
            .build();

        request.setAfter(after);
        DdlGeneratorResult generate = getDdlGeneratorResult(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes().stream()
            .filter(DialectNode::getExecutable)
            .collect(Collectors.toList());
        assertEquals("CREATE TABLE IF NOT EXISTS abc\n"
            + "(\n"
            + "   `add` DECIMAL(10,4) NOT NULL\n"
            + ")\n"
            + "COMMENT \"\\\"\\\"comment\";", dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining("\n")));
    }

    @Test
    public void testGeneratorCommentWithSingleQuote() {
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder()
            .dataType("decimal")
            .precision(10)
            .scale(4)
            .name("add")
            .nullable(true)
            .primaryKey(true)
            .build());

        Table after = Table.builder()
            .name("abc")
            .columns(columns)
            .comment("'\\comment'")
            .build();

        request.setAfter(after);
        DdlGeneratorResult generate = getDdlGeneratorResult(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes().stream()
            .filter(DialectNode::getExecutable)
            .collect(Collectors.toList());
        assertEquals("CREATE TABLE IF NOT EXISTS abc\n"
            + "(\n"
            + "   `add` DECIMAL(10,4) NULL\n"
            + ")\n"
            + "COMMENT \"'\\\\comment'\";", dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining("\n")));
    }

    @Test
    public void testDuplicateKey() {
        List<Column> columns = Lists.newArrayList();
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        columns.add(Column.builder()
            .dataType("tinyint")
            .name("c1")
            .nullable(false)
            .comment("ti_comment")
            .build());
        columns.add(Column.builder()
            .dataType("smallint")
            .name("c2")
            .nullable(false)
            .primaryKey(false)
            .comment("si_comment")
            .build());
        List<Constraint> constraints = Lists.newArrayList();
        ArrayList<String> es = Lists.newArrayList("c1");
        Constraint constraint = Constraint.builder().name(null)
            .columns(es)
            .type(ClientConstraintType.DUPLICATE_KEY)
            .build();
        constraints.add(constraint);
        Table after = Table.builder()
            .name("abc")
            .columns(columns)
            .comment("comment")
            .constraints(constraints)
            .build();
        request.setAfter(after);
        DdlGeneratorResult generate = getDdlGeneratorResult(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals(1, dialectNodes.size());
        assertEquals("CREATE TABLE IF NOT EXISTS abc\n"
            + "(\n"
            + "   c1 TINYINT NOT NULL COMMENT \"ti_comment\",\n"
            + "   c2 SMALLINT NOT NULL COMMENT \"si_comment\"\n"
            + ")\n"
            + "DUPLICATE KEY (c1)\n"
            + "COMMENT \"comment\";", dialectNodes.get(0).getNode());
    }

    @Test
    public void testAggregateKey() {
        List<Column> columns = Lists.newArrayList();
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        columns.add(Column.builder()
            .dataType("tinyint")
            .name("c1")
            .nullable(false)
            .comment("ti_comment")
            .build());
        columns.add(Column.builder()
            .dataType("smallint")
            .name("c2")
            .nullable(false)
            .primaryKey(false)
            .comment("si_comment")
            .build());
        List<Constraint> constraints = Lists.newArrayList();
        ArrayList<String> strings = Lists.newArrayList("c1", "c2");
        Constraint constraint = Constraint.builder().
            columns(strings)
            .type(ClientConstraintType.AGGREGATE_KEY)
            .build();
        constraints.add(constraint);
        Table after = Table.builder()
            .name("abc")
            .columns(columns)
            .comment("comment")
            .constraints(constraints)
            .build();
        request.setAfter(after);
        DdlGeneratorResult generate = getDdlGeneratorResult(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals(1, dialectNodes.size());
        assertEquals("CREATE TABLE IF NOT EXISTS abc\n"
            + "(\n"
            + "   c1 TINYINT NOT NULL COMMENT \"ti_comment\",\n"
            + "   c2 SMALLINT NOT NULL COMMENT \"si_comment\"\n"
            + ")\n"
            + "AGGREGATE KEY (c1,c2)\n"
            + "COMMENT \"comment\";", dialectNodes.get(0).getNode());
    }

    @Test
    public void testUniqueKey() {
        List<Column> columns = Lists.newArrayList();
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        columns.add(Column.builder()
            .dataType("tinyint")
            .name("c1")
            .nullable(false)
            .comment("ti_comment")
            .build());
        columns.add(Column.builder()
            .dataType("smallint")
            .name("c2")
            .nullable(false)
            .primaryKey(false)
            .comment("si_comment")
            .build());
        List<Constraint> constraints = Lists.newArrayList();
        ArrayList<String> strings = Lists.newArrayList("c1", "c2");
        Constraint constraint = Constraint.builder().columns(strings)
            .type(ClientConstraintType.UNIQUE_KEY)
            .build();
        constraints.add(constraint);
        Table after = Table.builder()
            .name("abc")
            .columns(columns)
            .comment("comment")
            .constraints(constraints)
            .build();
        request.setAfter(after);
        DdlGeneratorResult generate = getDdlGeneratorResult(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals(1, dialectNodes.size());
        assertEquals("CREATE TABLE IF NOT EXISTS abc\n"
            + "(\n"
            + "   c1 TINYINT NOT NULL COMMENT \"ti_comment\",\n"
            + "   c2 SMALLINT NOT NULL COMMENT \"si_comment\"\n"
            + ")\n"
            + "UNIQUE KEY (c1,c2)\n"
            + "COMMENT \"comment\";", dialectNodes.get(0).getNode());
    }

    @Test
    public void testDefaultValue() {
        List<Column> columns = Lists.newArrayList();
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        columns.add(Column.builder()
            .dataType("tinyint")
            .name("c1")
            .nullable(true)
            .comment("ti_comment")
            .defaultValue("0")
            .build());
        columns.add(Column.builder()
            .dataType("string")
            .name("c2")
            .nullable(false)
            .primaryKey(false)
            .comment("si_comment")
            .defaultValue("default_value")
            .build());

        columns.add(Column.builder()
            .dataType(DATETIME.getValue())
            .name("c3")
            .nullable(false)
            .primaryKey(false)
            .comment("si_comment")
            .defaultValue(CURRENT_TIMESTAMP)
            .build());

        columns.add(Column.builder()
            .dataType("string")
            .name("c4")
            .nullable(false)
            .primaryKey(false)
            .comment("si_comment")
            .defaultValue("NULL")
            .build());

        columns.add(Column.builder()
            .dataType("string")
            .name("c5")
            .nullable(false)
            .primaryKey(false)
            .comment("si_comment2")
            .defaultValue("test")
            .build());

        columns.add(Column.builder()
            .dataType("double")
            .name("c6")
            .nullable(false)
            .primaryKey(false)
            .comment("si_comment2")
            .defaultValue("3.14")
            .build());

        columns.add(Column.builder()
            .dataType(FLOAT.getValue())
            .name("c7")
            .nullable(false)
            .primaryKey(false)
            .comment("si_comment2")
            .defaultValue("0.14")
            .build());
        List<Constraint> constraints = Lists.newArrayList();
        Constraint constraint = Constraint.builder()
            .columns(Lists.newArrayList("c1", "c2"))
            .type(ClientConstraintType.UNIQUE_KEY)
            .build();
        constraints.add(constraint);
        Table after = Table.builder()
            .name("abc")
            .columns(columns)
            .comment("comment")
            .constraints(constraints)
            .build();
        request.setAfter(after);
        DdlGeneratorResult generate = getDdlGeneratorResult(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals(1, dialectNodes.size());
        String node = dialectNodes.get(0).getNode();
        assertEquals("CREATE TABLE IF NOT EXISTS abc\n"
            + "(\n"
            + "   c1 TINYINT NULL DEFAULT \"0\" COMMENT \"ti_comment\",\n"
            + "   c2 STRING NOT NULL DEFAULT \"default_value\" COMMENT \"si_comment\",\n"
            + "   c3 DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT \"si_comment\",\n"
            + "   c4 STRING NOT NULL DEFAULT NULL COMMENT \"si_comment\",\n"
            + "   c5 STRING NOT NULL DEFAULT \"test\" COMMENT \"si_comment2\",\n"
            + "   c6 DOUBLE NOT NULL DEFAULT \"3.14\" COMMENT \"si_comment2\",\n"
            + "   c7 FLOAT NOT NULL DEFAULT \"0.14\" COMMENT \"si_comment2\"\n"
            + ")\n"
            + "UNIQUE KEY (c1,c2)\n"
            + "COMMENT \"comment\";", node);
        CreateTable o = dorisLanguageParser.parseNode(node);
        assertNotNull(o);
    }
}
