package com.aliyun.fastmodel.transform.starrocks.client.generator;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.SetColumnOrder;
import com.aliyun.fastmodel.transform.api.client.CodeGenerator;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.property.StringProperty;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorModelRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.Constraint;
import com.aliyun.fastmodel.transform.api.extension.client.constraint.ExtensionClientConstraintType;
import com.aliyun.fastmodel.transform.api.client.generator.DefaultCodeGenerator;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.dialect.IVersion;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.starrocks.StarRocksTransformer;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 * ddl generator test
 *
 * @author panguanjing
 * @date 2023/9/17
 */
public class StarRocksGeneratorAlterTest {
    CodeGenerator codeGenerator = new DefaultCodeGenerator();

    @Test
    public void testGeneratorColumnOrderAfterPreservesDefinition() {
        Column anchor = Column.builder().name("anchor_value").dataType("int").nullable(false).build();
        Column originalNext = Column.builder().name("source_value").dataType("int").nullable(true).build();
        Column overall = Column.builder().name("status_value").dataType("varchar").length(256)
            .nullable(true).defaultValue("pending").comment("overall").build();
        Column summary = Column.builder().name("summary_value").dataType("varchar").length(16384)
            .nullable(false).comment("summary").build();

        List<DialectNode> nodes = generateColumnOrder(Lists.newArrayList(anchor, originalNext, overall, summary),
            Lists.newArrayList(anchor, overall, summary, originalNext));

        assertEquals(1, nodes.size());
        assertTrue(nodes.stream().allMatch(DialectNode::isExecutable));
        assertEquals("ALTER TABLE autotest.abc MODIFY COLUMN status_value VARCHAR(256) NULL"
            + " DEFAULT \"pending\" COMMENT \"overall\" AFTER anchor_value,"
            + " MODIFY COLUMN summary_value VARCHAR(16384) NOT NULL"
            + " COMMENT \"summary\" AFTER status_value", nodes.get(0).getNode());
    }

    @Test
    public void testGeneratorKeyOrderPreservesKeyMembership() {
        Column first = Column.builder().name("k1").dataType("int").nullable(false).build();
        Column moved = Column.builder().name("k2").dataType("int").nullable(false).comment("key").build();
        Table before = Table.builder().database("autotest").name("abc")
            .columns(Lists.newArrayList(first, moved)).constraints(Lists.newArrayList(Constraint.builder()
                .type(ExtensionClientConstraintType.DUPLICATE_KEY).columns(Lists.newArrayList("k1", "k2")).build()))
            .build();
        Table after = Table.builder().database("autotest").name("abc")
            .columns(Lists.newArrayList(moved, first)).constraints(before.getConstraints()).build();
        List<DialectNode> nodes = generate(before, after);

        assertEquals(1, nodes.size());
        assertTrue(nodes.get(0).isExecutable());
        assertEquals("ALTER TABLE autotest.abc MODIFY COLUMN k2 INT KEY NOT NULL COMMENT \"key\" FIRST",
            nodes.get(0).getNode());
    }

    @Test
    public void testGeneratorDropAndReorderUseOneSchemaChange() {
        Column first = Column.builder().name("a").dataType("int").nullable(false).build();
        Column second = Column.builder().name("b").dataType("int").nullable(true).build();
        Column dropped = Column.builder().name("c").dataType("int").nullable(true).build();
        Column moved = Column.builder().name("d").dataType("int").nullable(true).build();

        List<DialectNode> nodes = generateColumnOrder(Lists.newArrayList(first, second, dropped, moved),
            Lists.newArrayList(first, moved, second));

        assertEquals(1, nodes.size());
        assertTrue(nodes.get(0).isExecutable());
        assertEquals("ALTER TABLE autotest.abc DROP COLUMN c, MODIFY COLUMN d INT NULL AFTER a",
            nodes.get(0).getNode());
    }

    @Test
    public void testGeneratorTypeAndOrderChangePreserveFullTargetDefinition() {
        Column first = Column.builder().name("a").dataType("int").nullable(false).build();
        Column second = Column.builder().name("b").dataType("int").nullable(true).build();
        Column old = Column.builder().name("c").dataType("varchar").length(20).nullable(true)
            .defaultValue("seed").comment("payload").build();
        Column changed = Column.builder().name("c").dataType("varchar").length(40).nullable(true)
            .defaultValue("seed").comment("payload").build();

        List<DialectNode> nodes = generateColumnOrder(Lists.newArrayList(first, second, old),
            Lists.newArrayList(first, changed, second));

        assertEquals(1, nodes.size());
        assertTrue(nodes.get(0).isExecutable());
        assertTrue(nodes.get(0).getNode().contains("VARCHAR(40) NULL DEFAULT \"seed\" COMMENT \"payload\""));
        assertTrue(nodes.get(0).getNode().contains("AFTER a"));
        assertEquals(1, nodes.get(0).getNode().split("MODIFY COLUMN", -1).length - 1);
    }

    @Test
    public void testGeneratorColumnOrderFirstPreservesAggregation() {
        StringProperty aggregation = new StringProperty();
        aggregation.setKey("column_agg_desc");
        aggregation.setValueString("SUM");
        Column first = Column.builder().name("c1").dataType("int").nullable(false).build();
        Column moved = Column.builder().name("c2").dataType("int").nullable(true)
            .comment("metric").properties(Lists.newArrayList(aggregation)).build();

        List<DialectNode> nodes = generateColumnOrder(Lists.newArrayList(first, moved),
            Lists.newArrayList(moved, first));

        assertEquals(1, nodes.size());
        assertTrue(nodes.get(0).isExecutable());
        assertEquals("ALTER TABLE autotest.abc MODIFY COLUMN c2 INT SUM NULL COMMENT \"metric\" FIRST",
            nodes.get(0).getNode());
    }

    @Test
    public void testGeneratorUnchangedColumnOrderHasNoSql() {
        Column column = Column.builder().name("c1").dataType("int").nullable(false).build();

        assertTrue(generateColumnOrder(Lists.newArrayList(column), Lists.newArrayList(column)).isEmpty());
    }

    @Test
    public void testBatchDoesNotMergeDifferentTables() {
        StarRocksTransformer transformer = new StarRocksTransformer();
        Table target = Table.builder().name("abc").columns(Lists.newArrayList(
            Column.builder().name("c").dataType("int").nullable(true).build())).build();
        SetColumnOrder first = order("abc", "c", "c");
        SetColumnOrder other = order("other", "c", "c");
        List<DialectNode> nodes = transformer.transformStatements(Lists.newArrayList(first, other),
            transformer.reverseTable(target), TransformContext.builder().build());

        assertEquals(2, nodes.size());
        assertTrue(nodes.get(0).getNode().startsWith("ALTER TABLE abc "));
        assertTrue(nodes.get(1).getNode().startsWith("ALTER TABLE other "));
    }

    @Test
    public void testUnsupportedOrderRemainsAnExecutionBoundary() {
        StarRocksTransformer transformer = new StarRocksTransformer();
        Table target = Table.builder().name("abc").columns(Lists.newArrayList(
            Column.builder().name("c").dataType("int").nullable(true).build())).build();
        List<DialectNode> nodes = transformer.transformStatements(Lists.newArrayList(
            order("abc", "c", "c"), order("abc", "old", "renamed"), order("abc", "c", "c")),
            transformer.reverseTable(target), TransformContext.builder().build());

        assertEquals(3, nodes.size());
        assertTrue(nodes.get(0).isExecutable());
        assertFalse(nodes.get(1).isExecutable());
        assertTrue(nodes.get(2).isExecutable());
    }

    private SetColumnOrder order(String table, String oldName, String newName) {
        return new SetColumnOrder(QualifiedName.of(table), new Identifier(oldName), new Identifier(newName),
            new GenericDataType(new Identifier("int"), Lists.newArrayList()), null, true);
    }

    private List<DialectNode> generateColumnOrder(List<Column> beforeColumns, List<Column> afterColumns) {
        Table before = Table.builder().database("autotest").name("abc").columns(beforeColumns).build();
        Table after = Table.builder().database("autotest").name("abc").columns(afterColumns).build();
        return generate(before, after);
    }

    private List<DialectNode> generate(Table before, Table after) {
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder().before(before).after(after)
            .config(TableConfig.builder()
                .dialectMeta(DialectMeta.getByNameAndVersion(DialectName.STARROCKS.getValue(), IVersion.DEFAULT_VERSION))
                .build()).build();
        return codeGenerator.generate(request).getDialectNodes();
    }

    @Test
    public void testGeneratorAddColumn() {
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        List<Column> oneColumns = Lists.newArrayList();
        Column e1 = Column.builder()
            .dataType("int")
            .name("c1")
            .nullable(true)
            .build();
        oneColumns.add(e1);

        List<Column> twoColumns = Lists.newArrayList();
        twoColumns.add(e1);
        twoColumns.add(Column.builder()
            .dataType("int")
            .name("c2")
            .comment("comment")
            .build());
        Table after = Table.builder()
            .database("autotest")
            .name("abc")
            .columns(twoColumns)
            .comment("comment")
            .build();
        request.setAfter(after);
        Table before = Table.builder()
            .database("autotest")
            .name("abc")
            .columns(oneColumns)
            .comment("comment2")
            .build();
        request.setBefore(before);
        request.setConfig(TableConfig.builder()
            .dialectMeta(DialectMeta.getByNameAndVersion(DialectName.STARROCKS.getValue(), IVersion.DEFAULT_VERSION))
            .build());
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals("ALTER TABLE autotest.abc ADD COLUMN\n"
                + "(\n"
                + "   c2 INT NOT NULL COMMENT \"comment\"\n"
                + ");\n"
                + "ALTER TABLE autotest.abc SET COMMENT \"comment\"",
            dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining(";\n")));
    }

    @Test
    public void testGeneratorDropColumn() {
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        List<Column> beforeColumns = Lists.newArrayList();

        beforeColumns.add(Column.builder()
            .dataType("int")
            .name("c1")
            .nullable(false)
            .build());

        Column build = Column.builder()
            .dataType("int")
            .name("c2")
            .partitionKeyIndex(0)
            .partitionKey(true)
            .comment("comment")
            .build();
        beforeColumns.add(build);

        List<Column> afterColumns = Lists.newArrayList();
        afterColumns.add(build);

        Table before = Table.builder()
            .database("autotest")
            .name("abc")
            .columns(beforeColumns)
            .comment("comment1")
            .build();

        Table after = Table.builder()
            .database("autotest")
            .name("abc")
            .columns(afterColumns)
            .build();

        request.setBefore(before);
        request.setAfter(after);
        request.setConfig(TableConfig.builder()
            .dialectMeta(DialectMeta.getByNameAndVersion(DialectName.STARROCKS.getValue(), IVersion.DEFAULT_VERSION))
            .build());
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals("ALTER TABLE autotest.abc DROP COLUMN c1,\n"
            + "ALTER TABLE autotest.abc SET COMMENT \"\"", dialectNodes.stream().filter(DialectNode::isExecutable).map(DialectNode::getNode)
            .collect(Collectors.joining(",\n")));
    }

    @Test
    public void testGeneratorModifyColumn() {
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        List<Column> beforeColumns = Lists.newArrayList();

        beforeColumns.add(Column.builder()
            .dataType("int")
            .name("c1")
            .nullable(false)
            .build());

        Column build = Column.builder()
            .dataType("int")
            .name("c1")
            .comment("comment")
            .nullable(true)
            .build();

        List<Column> afterColumns = Lists.newArrayList();
        afterColumns.add(build);

        Table before = Table.builder()
            .database("autotest")
            .name("abc")
            .columns(beforeColumns)
            .build();

        Table after = Table.builder()
            .database("autotest")
            .name("abc")
            .columns(afterColumns)
            .build();

        request.setBefore(before);
        request.setAfter(after);
        request.setConfig(TableConfig.builder()
            .dialectMeta(DialectMeta.getByNameAndVersion(DialectName.STARROCKS.getValue(), IVersion.DEFAULT_VERSION))
            .build());
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals("ALTER TABLE autotest.abc MODIFY COLUMN c1 INT NULL COMMENT \"comment\"", dialectNodes.stream().filter(DialectNode::isExecutable)
            .map(DialectNode::getNode)
            .collect(Collectors.joining(",\n")));
    }

    @Test
    public void testGeneratorSetProperties() {
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        List<Column> beforeColumns = Lists.newArrayList();

        beforeColumns.add(Column.builder()
            .dataType("int")
            .name("c1")
            .nullable(false)
            .build());

        StringProperty stringProperty = new StringProperty();
        stringProperty.setKey("default.replication_num");
        stringProperty.setValueString("2");
        List<BaseClientProperty> properties = Lists.newArrayList(
            stringProperty
        );
        Table before = Table.builder()
            .database("autotest")
            .name("abc")
            .columns(beforeColumns)
            .build();

        Table after = Table.builder()
            .database("autotest")
            .name("abc")
            .columns(beforeColumns)
            .properties(properties)
            .build();

        request.setBefore(before);
        request.setAfter(after);
        request.setConfig(TableConfig.builder()
            .dialectMeta(DialectMeta.getByNameAndVersion(DialectName.STARROCKS.getValue(), IVersion.DEFAULT_VERSION))
            .build());
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals("ALTER TABLE autotest.abc SET (\"default.replication_num\"=\"2\")", dialectNodes.stream().filter(DialectNode::isExecutable)
            .map(DialectNode::getNode)
            .collect(Collectors.joining(",\n")));

    }

    @Test
    public void testGeneratorUnSetProperties() {
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        List<Column> beforeColumns = Lists.newArrayList();

        beforeColumns.add(Column.builder()
            .dataType("int")
            .name("c1")
            .nullable(false)
            .build());

        StringProperty stringProperty = new StringProperty();
        stringProperty.setKey("default.replication_num");
        stringProperty.setValueString("2");
        List<BaseClientProperty> properties = Lists.newArrayList(
            stringProperty
        );
        Table before = Table.builder()
            .database("autotest")
            .name("abc")
            .columns(beforeColumns)
            .properties(properties)
            .build();

        Table after = Table.builder()
            .database("autotest")
            .name("abc")
            .columns(beforeColumns)
            .build();

        request.setBefore(before);
        request.setAfter(after);
        request.setConfig(TableConfig.builder()
            .dialectMeta(DialectMeta.getByNameAndVersion(DialectName.STARROCKS.getValue(), IVersion.DEFAULT_VERSION))
            .build());
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals("ALTER TABLE autotest.abc SET (\"default.replication_num\"=\"\")", dialectNodes.stream().filter(DialectNode::isExecutable)
            .map(DialectNode::getNode)
            .collect(Collectors.joining(",\n")));

    }
}
