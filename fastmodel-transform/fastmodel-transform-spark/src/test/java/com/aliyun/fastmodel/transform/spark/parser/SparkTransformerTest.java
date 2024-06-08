package com.aliyun.fastmodel.transform.spark.parser;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.spark.SparkTransformer;
import com.aliyun.fastmodel.transform.spark.context.SparkTableFormat;
import com.aliyun.fastmodel.transform.spark.context.SparkTransformContext;
import com.aliyun.fastmodel.transform.spark.format.SparkPropertyKey;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/2/13
 */
public class SparkTransformerTest {

    @Test
    public void testTransform() {
        SparkTransformer sparkTransform = new SparkTransformer();
        BaseStatement source = CreateTable.builder().tableName(QualifiedName.of("abc")).build();
        DialectNode transform = sparkTransform.transform(source);
        assertEquals(transform.getNode(), "CREATE TABLE abc");
    }

    @Test
    public void testTransformWithColumns() {
        SparkTransformer sparkTransformer = new SparkTransformer();
        List<Property> properties = Lists.newArrayList();
        properties.add(new Property(SparkPropertyKey.CLUSTERED_BY.getValue(), "id"));
        List<ColumnDefinition> columns = Lists.newArrayList();
        columns.add(ColumnDefinition.builder().colName(new Identifier("id")).dataType(DataTypeUtil.simpleType("bigint", null))
            .comment(new Comment("c1")).build());
        BaseStatement source = CreateTable.builder()
            .tableName(QualifiedName.of("tb")).columns(columns).comment(new Comment("abc")).properties(
                properties).build();
        DialectNode transform = sparkTransformer.transform(source,
            SparkTransformContext.builder().tableFormat(SparkTableFormat.HIVE_FORMAT).build());
        assertEquals(transform.getNode(), "CREATE TABLE tb\n"
            + "(\n"
            + "   id BIGINT COMMENT 'c1'\n"
            + ")\n"
            + "COMMENT 'abc'\n"
            + "CLUSTERED BY (id)");
    }

    @Test
    public void testTransformWithCsv() {
        SparkTransformer sparkTransformer = new SparkTransformer();
        List<Property> properties = Lists.newArrayList();
        properties.add(new Property(SparkPropertyKey.CLUSTERED_BY.getValue(), "id"));
        properties.add(new Property(SparkPropertyKey.USING.getValue(), "CSV"));
        List<ColumnDefinition> columns = Lists.newArrayList();
        columns.add(ColumnDefinition.builder().colName(new Identifier("id")).dataType(DataTypeUtil.simpleType("bigint", null))
            .comment(new Comment("c1")).build());
        BaseStatement source = CreateTable.builder()
            .tableName(QualifiedName.of("tb")).columns(columns).comment(new Comment("abc")).properties(
                properties).build();
        DialectNode transform = sparkTransformer.transform(source,
            SparkTransformContext.builder().tableFormat(SparkTableFormat.DATASOURCE_FORMAT).build());
        assertEquals(transform.getNode(), "CREATE TABLE tb\n"
            + "(\n"
            + "   id BIGINT COMMENT 'c1'\n"
            + ")\n"
            + "USING CSV\n"
            + "CLUSTERED BY (id)\n"
            + "COMMENT 'abc'");
    }

    @Test
    public void testReverse() {
        SparkTransformer sparkTransformer = new SparkTransformer();
        BaseStatement reverse = sparkTransformer.reverse(new DialectNode("create table a (b bigint) comment 'abc';"));
        assertEquals(reverse.toString(), "CREATE TABLE a \n"
            + "(\n"
            + "   b BIGINT\n"
            + ")\n"
            + "COMMENT 'abc'");
    }
}