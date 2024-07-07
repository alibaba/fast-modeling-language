package com.aliyun.fastmodel.transform.adbmysql.format;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.adbmysql.context.AdbMysqlTransformContext;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.DistributeConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.ExpressionPartitionBy;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * AdbMysqlVisitorTest
 *
 * @author panguanjing
 * @date 2023/2/11
 */
public class AdbMysqlOutVisitorTest {

    @Test
    public void visitCreateTable() {
        AdbMysqlTransformContext context = AdbMysqlTransformContext.builder().build();
        AdbMysqlOutVisitor adbMysqlVisitor = new AdbMysqlOutVisitor(context);
        List<ColumnDefinition> columns = Lists.newArrayList();
        columns.add(ColumnDefinition.builder().colName(new Identifier("c1")).dataType(DataTypeUtil.simpleType("BIGINT", null)).build());
        columns.add(ColumnDefinition.builder().colName(new Identifier("c2")).dataType(DataTypeUtil.simpleType("BIGINT", null)).build());
        List<ColumnDefinition> columns2 = Lists.newArrayList();
        columns2.add(ColumnDefinition.builder().colName(new Identifier("c2")).dataType(DataTypeUtil.simpleType("BIGINT", null)).build());
        List<BaseExpression> arguments = Lists.newArrayList(
            new TableOrColumn(QualifiedName.of("c2")),
            new StringLiteral("%Y%m%d")
        );
        FunctionCall dateFormat = new FunctionCall(
            QualifiedName.of("DATE_FORMAT"),
            false, arguments
        );
        PartitionedBy partitions = new ExpressionPartitionBy(Lists.newArrayList(), dateFormat, null);
        List<Property> properties = Lists.newArrayList();
        properties.add(new Property(AdbMysqlPropertyKey.LIFE_CYCLE.getValue(), "10"));
        properties.add(new Property(AdbMysqlPropertyKey.BLOCK_SIZE.getValue(), "10"));
        properties.add(new Property(AdbMysqlPropertyKey.STORAGE_POLICY.getValue(), "MIXED"));
        properties.add(new Property(AdbMysqlPropertyKey.HOT_PARTITION_COUNT.getValue(), "10"));
        List<BaseConstraint> constraints = Lists.newArrayList();
        ArrayList<Identifier> es = Lists.newArrayList(new Identifier("c1"), new Identifier("c2"));
        constraints.add(new DistributeConstraint(es, null));
        CreateTable node = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .columns(columns)
            .partition(partitions)
            .constraints(constraints)
            .properties(properties)
            .build();
        adbMysqlVisitor.visitCreateTable(node, 0);

        String s = adbMysqlVisitor.getBuilder().toString();
        assertEquals(s, "CREATE TABLE abc\n"
            + "(\n"
            + "   c1 BIGINT,\n"
            + "   c2 BIGINT\n"
            + ")\n"
            + "DISTRIBUTE BY HASH(c1,c2)\n"
            + "PARTITION BY VALUE(DATE_FORMAT(c2, '%Y%m%d')) LIFECYCLE 10\n"
            + "BLOCK_SIZE=10\n"
            + "STORAGE_POLICY='MIXED'\n"
            + "HOT_PARTITION_COUNT=10");
    }

    @Test
    public void visitCreateTableWithoutDateFormat() {
        AdbMysqlTransformContext context = AdbMysqlTransformContext.builder().build();
        AdbMysqlOutVisitor adbMysqlVisitor = new AdbMysqlOutVisitor(context);
        List<ColumnDefinition> columns = Lists.newArrayList();
        columns.add(ColumnDefinition.builder().colName(new Identifier("c1")).dataType(DataTypeUtil.simpleType("BIGINT", null)).build());
        columns.add(ColumnDefinition.builder().colName(new Identifier("c2")).dataType(DataTypeUtil.simpleType("BIGINT", null)).build());
        List<ColumnDefinition> columns2 = Lists.newArrayList();
        columns2.add(ColumnDefinition.builder().colName(new Identifier("c2")).dataType(DataTypeUtil.simpleType("BIGINT", null)).build());
        PartitionedBy partitions = new PartitionedBy(columns2);
        List<Property> properties = Lists.newArrayList();
        properties.add(new Property(AdbMysqlPropertyKey.LIFE_CYCLE.getValue(), "10"));

        properties.add(new Property(AdbMysqlPropertyKey.BLOCK_SIZE.getValue(), "10"));
        properties.add(new Property(AdbMysqlPropertyKey.STORAGE_POLICY.getValue(), "MIXED"));
        properties.add(new Property(AdbMysqlPropertyKey.HOT_PARTITION_COUNT.getValue(), "10"));
        List<BaseConstraint> constraints = Lists.newArrayList();
        ArrayList<Identifier> es = Lists.newArrayList(new Identifier("c1"), new Identifier("c2"));
        constraints.add(new DistributeConstraint(es, null));
        CreateTable node = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .columns(columns)
            .partition(partitions)
            .properties(properties)
            .constraints(constraints)
            .build();
        adbMysqlVisitor.visitCreateTable(node, 0);

        String s = adbMysqlVisitor.getBuilder().toString();
        assertEquals(s, "CREATE TABLE abc\n"
            + "(\n"
            + "   c1 BIGINT,\n"
            + "   c2 BIGINT\n"
            + ")\n"
            + "DISTRIBUTE BY HASH(c1,c2)\n"
            + "PARTITION BY VALUE(c2) LIFECYCLE 10\n"
            + "BLOCK_SIZE=10\n"
            + "STORAGE_POLICY='MIXED'\n"
            + "HOT_PARTITION_COUNT=10");
    }
}