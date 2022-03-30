/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.compare.impl;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.compare.BaseCompareNode;
import com.aliyun.fastmodel.compare.ComparePair;
import com.aliyun.fastmodel.compare.CompareStrategy;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * CompositeCompareNodeTest
 *
 * @author panguanjing
 * @date 2021/8/31
 */
public class CompositeCompareNodeTest {

    CompositeCompareNode compositeCompareNode;

    @Before
    public void setUp() throws Exception {
        Map<String, BaseCompareNode> map = Maps.newHashMap();
        map.put(CreateTable.class.getName(), new CreateTableCompareNode());
        compositeCompareNode = new CompositeCompareNode(map);
    }

    @Test
    public void compareResult() {
        List<BaseStatement> statements = ImmutableList.of(
            CreateTable.builder()
                .tableName(QualifiedName.of("a"))
                .detailType(TableDetailType.NORMAL_DIM).build()
        );
        List<ColumnDefinition> list = ImmutableList.of(
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .build()
        );

        List<BaseStatement> before = Lists.newArrayList(
            CreateTable.builder()
                .tableName(QualifiedName.of("a"))
                .columns(list)
                .detailType(TableDetailType.NORMAL_DIM).build()
        );
        List<BaseStatement> baseStatementList = compositeCompareNode.compareResult(new CompositeStatement(before),
            new CompositeStatement(
                statements
            ), CompareStrategy.INCREMENTAL);
        String collect = baseStatementList.stream().map(BaseStatement::toString).collect(Collectors.joining(";\n"));
        assertEquals(collect, "ALTER TABLE a DROP COLUMN c1");
    }

    @Test
    public void testCompareResultWithSort() {
        List<BaseStatement> before = Lists.newArrayList(
            CreateTable.builder()
                .tableName(QualifiedName.of("c"))
                .detailType(TableDetailType.NORMAL_DIM)
                .build(),
            CreateTable.builder()
                .tableName(QualifiedName.of("b"))
                .detailType(TableDetailType.NORMAL_DIM)
                .build(),
            CreateTable.builder()
                .tableName(QualifiedName.of("a"))
                .detailType(TableDetailType.NORMAL_DIM)
                .build()
        );

        List<BaseStatement> after = Lists.newArrayList(
            CreateTable.builder()
                .tableName(QualifiedName.of("x"))
                .detailType(TableDetailType.NORMAL_DIM).build(),
            CreateTable.builder()
                .tableName(QualifiedName.of("b"))
                .detailType(TableDetailType.NORMAL_DIM).build(),
            CreateTable.builder()
                .tableName(QualifiedName.of("c"))
                .detailType(TableDetailType.NORMAL_DIM).build()

        );
        List<BaseStatement> baseStatementList = compositeCompareNode.compareResult(new CompositeStatement(before),
            new CompositeStatement(
                after
            ), CompareStrategy.INCREMENTAL);
        String collect = baseStatementList.stream().map(BaseStatement::toString).collect(Collectors.joining(";\n"));
        assertEquals("ALTER TABLE a RENAME TO x", collect);
    }

    @Test
    public void testPrepareCompare() {
        CreateTable c = CreateTable.builder()
            .tableName(QualifiedName.of("a"))
            .build();
        CreateTable d = CreateTable.builder()
            .tableName(QualifiedName.of("b"))
            .build();
        ComparePair comparePair = compositeCompareNode.prepareCompare(Optional.of(c),
            Optional.of(new CompositeStatement(Arrays.asList(d))));
        assertTrue(comparePair.getLeft().isPresent());
        assertTrue(comparePair.getRight().isPresent());
        Optional<Node> left = comparePair.getLeft();
        assertEquals(left.get().getClass(), CompositeStatement.class);
    }
}