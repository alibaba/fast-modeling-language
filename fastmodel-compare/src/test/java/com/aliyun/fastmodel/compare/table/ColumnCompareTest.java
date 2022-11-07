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

package com.aliyun.fastmodel.compare.table;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.compare.impl.table.ColumnCompare;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.ColumnCategory;
import com.aliyun.fastmodel.core.tree.statement.constants.ColumnPropertyDefaultKey;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * ColumnCompareTest
 *
 * @author panguanjing
 * @date 2021/9/13
 */
public class ColumnCompareTest {
    ColumnCompare columnCompare = new ColumnCompare();

    @Test
    public void testCompare() {
        ColumnDefinition c1 = ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .category(ColumnCategory.REL_DIMENSION)
            .refDimension(QualifiedName.of("dim_shop"))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
            .build();
        CreateTable before = CreateTable.builder().columns(
            ImmutableList.of(c1)
        ).tableName(QualifiedName.of("dim_shop")).build();

        ColumnDefinition c2 = ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
            .build();
        CreateTable after = CreateTable.builder().columns(
            ImmutableList.of(c2)
        ).tableName(QualifiedName.of("dim_shop")).build();
        List<BaseStatement> baseStatementList = columnCompare.compareTableElement(before, after);
        assertEquals(baseStatementList.toString(), "[ALTER TABLE dim_shop CHANGE COLUMN c1 c1 BIGINT]");
    }

    @Test
    public void testCompareIndicator() {
        ColumnDefinition c1 = ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .category(ColumnCategory.REL_INDICATOR)
            .refIndicators(ImmutableList.of(new Identifier("c1")))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
            .build();
        CreateTable before = CreateTable.builder().columns(
            ImmutableList.of(c1)
        ).tableName(QualifiedName.of("dim_shop")).build();

        ColumnDefinition c2 = ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
            .refIndicators(ImmutableList.of(new Identifier("c2"), new Identifier("c3")))
            .build();
        CreateTable after = CreateTable.builder().columns(
            ImmutableList.of(c2)
        ).tableName(QualifiedName.of("dim_shop")).build();
        List<BaseStatement> baseStatementList = columnCompare.compareTableElement(before, after);
        assertEquals(baseStatementList.toString(),
            "[ALTER TABLE dim_shop CHANGE COLUMN c1 c1 BIGINT REFERENCES (c2,c3)]");
    }

    @Test
    public void testCompareAfter() {
        ColumnDefinition c1 = ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
            .category(ColumnCategory.REL_DIMENSION)
            .build();
        CreateTable before = CreateTable.builder().columns(
            ImmutableList.of(c1)
        ).tableName(QualifiedName.of("dim_shop")).build();

        ColumnDefinition c2 = ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
            .refDimension(QualifiedName.of("dim_shop.c1"))
            .build();
        CreateTable after = CreateTable.builder().columns(
            ImmutableList.of(c2)
        ).tableName(QualifiedName.of("dim_shop")).build();
        List<BaseStatement> baseStatementList = columnCompare.compareTableElement(before, after);
        assertEquals(baseStatementList.toString(),
            "[ALTER TABLE dim_shop CHANGE COLUMN c1 c1 BIGINT REFERENCES dim_shop.c1]");
    }

    @Test
    public void compareTableElement() {
        ColumnDefinition c1 = ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .category(ColumnCategory.ATTRIBUTE)
            .build();
        CreateTable before = CreateTable.builder().columns(
            ImmutableList.of(c1)
        ).tableName(QualifiedName.of("dim_shop")).build();

        ColumnDefinition c2 = ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .build();
        CreateTable after = CreateTable.builder().columns(
            ImmutableList.of(c2)
        ).tableName(QualifiedName.of("dim_shop")).build();
        List<BaseStatement> baseStatementList = columnCompare.compareTableElement(before, after);
        assertEquals(0, baseStatementList.size());
    }

    @Test
    public void compareOrder() {
        ColumnDefinition columnDefinition = ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
            .build();
        ColumnDefinition columnDefinition2 = ColumnDefinition.builder()
            .colName(new Identifier("c2"))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
            .build();
        CreateTable before = CreateTable.builder()
            .columns(ImmutableList.of(columnDefinition, columnDefinition2))
            .tableName(QualifiedName.of("dim_shop")).build();

        CreateTable after = CreateTable.builder()
            .columns(ImmutableList.of(columnDefinition2, columnDefinition))
            .tableName(QualifiedName.of("dim_shop")).build();

        List<BaseStatement> baseStatementList = columnCompare.compareTableElement(before, after);
        String join = Joiner.on("\n").join(baseStatementList);
        assertEquals(join, "ALTER TABLE dim_shop CHANGE COLUMN c2 c2 BIGINT FIRST" );
    }

    @Test
    public void testCompareNode2() {

        List<ColumnDefinition> beforeColumns = Lists.newArrayList(
            getColumn("b"),
            getColumn("a"),
            getColumn("d"),
            getColumn("c"),
            getColumn("f"),
            getColumn("e"),
            getColumn("h"),
            getColumn("g")
        );

        List<ColumnDefinition> afterColumns = Lists.newArrayList(
            getColumn("h"),
            getColumn("g"),
            getColumn("f"),
            getColumn("e"),
            getColumn("d"),
            getColumn("c"),
            getColumn("b"),
            getColumn("a")
        );
        CreateTable before = CreateTable.builder()
            .columns(beforeColumns)
            .tableName(QualifiedName.of("dim_shop")).build();


        CreateTable after = CreateTable.builder()
            .columns(afterColumns)
            .tableName(QualifiedName.of("dim_shop")).build();

        List<BaseStatement> baseStatements = columnCompare.compareTableElement(before, after);
        String s = baseStatements.stream().map(BaseStatement::toString).collect(Collectors.joining(";\n"));
        assertEquals(s, "ALTER TABLE dim_shop CHANGE COLUMN h h BIGINT FIRST;\n"
            + "ALTER TABLE dim_shop CHANGE COLUMN g g BIGINT AFTER h;\n"
            + "ALTER TABLE dim_shop CHANGE COLUMN f f BIGINT AFTER g;\n"
            + "ALTER TABLE dim_shop CHANGE COLUMN e e BIGINT AFTER f;\n"
            + "ALTER TABLE dim_shop CHANGE COLUMN d d BIGINT AFTER e;\n"
            + "ALTER TABLE dim_shop CHANGE COLUMN c c BIGINT AFTER d");
    }


    private static ColumnDefinition getColumn(String colName) {
        return ColumnDefinition.builder().colName(new Identifier(colName))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
            .build();
    }

    @Test
    public void compareOrderDropColumn() {
        ColumnDefinition columnDefinition = ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
            .build();
        ColumnDefinition columnDefinition2 = ColumnDefinition.builder()
            .colName(new Identifier("c2"))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
            .build();
        CreateTable before = CreateTable.builder()
            .columns(ImmutableList.of(columnDefinition, columnDefinition2))
            .tableName(QualifiedName.of("dim_shop")).build();

        CreateTable after = CreateTable.builder()
            .columns(ImmutableList.of(columnDefinition2))
            .tableName(QualifiedName.of("dim_shop")).build();

        List<BaseStatement> baseStatementList = columnCompare.compareTableElement(before, after);
        String join = Joiner.on("\n").join(baseStatementList);
        assertEquals(join, "ALTER TABLE dim_shop DROP COLUMN c1\n"
            + "ALTER TABLE dim_shop CHANGE COLUMN c2 c2 BIGINT FIRST");

    }

    @Test
    public void testCompareProperties() {
        ColumnDefinition columnDefinition = ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
            .properties(Lists.newArrayList(new Property(ColumnPropertyDefaultKey.code_table.name(), "a")))
            .build();
        ColumnDefinition columnDefinition2 = ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
            .properties(Lists.newArrayList(new Property(ColumnPropertyDefaultKey.code_table.name(), "b")))
            .build();
        CreateTable before = CreateTable.builder()
            .columns(ImmutableList.of(columnDefinition))
            .tableName(QualifiedName.of("dim_shop")).build();

        CreateTable after = CreateTable.builder()
            .columns(ImmutableList.of(columnDefinition2))
            .tableName(QualifiedName.of("dim_shop")).build();

        List<BaseStatement> baseStatementList = columnCompare.compareTableElement(before, after);
        assertEquals(1, baseStatementList.size());
        String join = Joiner.on("\n").join(baseStatementList);
        assertEquals(join, "ALTER TABLE dim_shop CHANGE COLUMN c1 c1 BIGINT WITH ('code_table'='b')");
    }

    @Test
    public void testComparePropertiesAfterNull() {
        ColumnDefinition columnDefinition = ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
            .properties(Lists.newArrayList(new Property(ColumnPropertyDefaultKey.code_table.name(), "a")))
            .build();
        ColumnDefinition columnDefinition2 = ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
            .build();
        CreateTable before = CreateTable.builder()
            .columns(ImmutableList.of(columnDefinition))
            .tableName(QualifiedName.of("dim_shop")).build();

        CreateTable after = CreateTable.builder()
            .columns(ImmutableList.of(columnDefinition2))
            .tableName(QualifiedName.of("dim_shop")).build();

        List<BaseStatement> baseStatementList = columnCompare.compareTableElement(before, after);
        assertEquals(1, baseStatementList.size());
        String join = Joiner.on("\n").join(baseStatementList);
        assertEquals(join, "ALTER TABLE dim_shop CHANGE COLUMN c1 c1 BIGINT WITH ('code_table'='')");
    }

    @Test
    public void testComparePropertiesBeforeNull() {
        ColumnDefinition columnDefinition = ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
            .build();
        ColumnDefinition columnDefinition2 = ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
            .properties(Lists.newArrayList(new Property(ColumnPropertyDefaultKey.code_table.name(), "a")))
            .build();
        CreateTable before = CreateTable.builder()
            .columns(ImmutableList.of(columnDefinition))
            .tableName(QualifiedName.of("dim_shop")).build();

        CreateTable after = CreateTable.builder()
            .columns(ImmutableList.of(columnDefinition2))
            .tableName(QualifiedName.of("dim_shop")).build();

        List<BaseStatement> baseStatementList = columnCompare.compareTableElement(before, after);
        assertEquals(1, baseStatementList.size());
        String join = Joiner.on("\n").join(baseStatementList);
        assertEquals(join, "ALTER TABLE dim_shop CHANGE COLUMN c1 c1 BIGINT WITH ('code_table'='a')");
    }
}