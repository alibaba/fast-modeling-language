/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.mc.reverse;

import com.aliyun.fastmodel.core.exception.SemanticException;
import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.parser.NodeParser;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.mc.MaxComputeTransformer;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * reverse builder
 *
 * @author panguanjing
 * @date 2022/4/12
 */
public class MaxComputeAstBuilderTest {

    MaxComputeTransformer maxComputeTransformer = new MaxComputeTransformer();

    NodeParser nodeParser = new NodeParser();

    @Test
    public void testReverse() {
        String code = "create table a (s bigint, d string) comment 'abc';";
        ReverseContext context = ReverseContext.builder()
            .columnProperty(ColumnDefinition.builder().colName(new Identifier("s")).primary(true).build())
            .columnProperty(ColumnDefinition.builder().colName(new Identifier("d")).primary(true).build())
            .build();
        BaseStatement reverse = maxComputeTransformer.reverse(new DialectNode(code), context);
        String actual = reverse.toString();
        MatcherAssert.assertThat(actual, Matchers.equalTo("CREATE DIM TABLE a \n"
            + "(\n"
            + "   s BIGINT,\n"
            + "   d STRING,\n"
            + "   PRIMARY KEY(s,d)\n"
            + ")\n"
            + "COMMENT 'abc'"));
        BaseStatement baseStatement = nodeParser.parseStatement(actual);
        assertNotNull(baseStatement);
    }

    @Test
    public void testReversePartition() {
        String code = "create table a (s bigint, d string) comment 'abc' partitioned by (ds string);";
        ReverseContext context = ReverseContext.builder()
            .columnProperty(ColumnDefinition.builder().colName(new Identifier("s")).primary(true).build())
            .columnProperty(ColumnDefinition.builder().colName(new Identifier("d")).primary(true).build())
            .columnProperty(ColumnDefinition.builder().colName(new Identifier("ds")).primary(true).build())
            .build();
        BaseStatement reverse = maxComputeTransformer.reverse(new DialectNode(code), context);
        String actual = reverse.toString();
        MatcherAssert.assertThat(actual, Matchers.equalTo("CREATE DIM TABLE a \n"
            + "(\n"
            + "   s BIGINT,\n"
            + "   d STRING,\n"
            + "   PRIMARY KEY(s,d,ds)\n"
            + ")\n"
            + "COMMENT 'abc'\n"
            + "PARTITIONED BY\n"
            + "(\n"
            + "   ds STRING\n"
            + ")"));
        BaseStatement baseStatement = nodeParser.parseStatement(actual);
        assertNotNull(baseStatement);
    }

    @Test(expected = SemanticException.class)
    public void testReversePartitionWithSameColumn() {
        String code = "create table a (s bigint, d string) comment 'abc' partitioned by (s string);";
        ReverseContext context = ReverseContext.builder()
            .columnProperty(ColumnDefinition.builder().colName(new Identifier("s")).primary(true).build())
            .columnProperty(ColumnDefinition.builder().colName(new Identifier("d")).primary(true).build())
            .build();
        maxComputeTransformer.reverse(new DialectNode(code), context);
    }

    @Test
    public void testReverseAlias() {
        String code = "create table a (s bigint, d string) comment 'abc';";
        ReverseContext context = ReverseContext.builder()
            .columnProperty(ColumnDefinition.builder().colName(new Identifier("s")).aliasedName(new AliasedName("ddd")).build())
            .columnProperty(ColumnDefinition.builder().colName(new Identifier("d")).build())
            .build();
        BaseStatement reverse = maxComputeTransformer.reverse(new DialectNode(code), context);
        MatcherAssert.assertThat(reverse.toString(), Matchers.equalTo("CREATE DIM TABLE a \n"
            + "(\n"
            + "   s ALIAS 'ddd' BIGINT,\n"
            + "   d STRING\n"
            + ")\n"
            + "COMMENT 'abc'"));
    }

}