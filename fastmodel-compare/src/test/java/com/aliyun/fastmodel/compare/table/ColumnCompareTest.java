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

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.ColumnCategory;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
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
        assertEquals(join, "ALTER TABLE dim_shop CHANGE COLUMN c1 c1 BIGINT AFTER c2");
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
        assertEquals(join, "ALTER TABLE dim_shop DROP COLUMN c1");

    }
}