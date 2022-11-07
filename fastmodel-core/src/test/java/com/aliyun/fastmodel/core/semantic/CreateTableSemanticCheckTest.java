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

package com.aliyun.fastmodel.core.semantic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.aliyun.fastmodel.core.exception.SemanticException;
import com.aliyun.fastmodel.core.semantic.table.CreateTableSemanticCheck;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.DimConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * @author panguanjing
 * @date 2020/9/17
 */
public class CreateTableSemanticCheckTest {
    public static final Identifier COL_1 = new Identifier("col1");
    CreateTableSemanticCheck createTableSemanticCheck = new CreateTableSemanticCheck();

    @Test
    public void testCheckColNameNotExist() throws Exception {
        List<ColumnDefinition> list = constructorTypeStatement();
        List<BaseConstraint> baseConstraints = constructorConstraintStatement();
        CreateTable baseStatement = CreateDimTable.builder().tableName(
            QualifiedName.of("name")).columns(list).constraints(baseConstraints).comment(
            new Comment("comment")
        ).build();
        try {
            createTableSemanticCheck.check(baseStatement);
        } catch (SemanticException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void testCheckColNameDuplicate() {
        try {
            List<ColumnDefinition> duplicateList = constructorDuplicateStatement();
            CreateDimTable.builder().tableName(
                QualifiedName.of("name")).columns(duplicateList).build();
        } catch (SemanticException e) {
            assertNotNull(e);
            assertTrue(e.getMessage().contains("duplicate"));
        }
    }

    @Test
    public void testPrimaryKeyMultiKey() {
        List<ColumnDefinition> columnDefinitions = constructorTypeStatement();
        try {
            CreateDimTable.builder().tableName(
                    QualifiedName.of("a.b")).columns(columnDefinitions).constraints(constructConstraintMultiPrimary())
                .build();
        } catch (SemanticException e) {
            assertNotNull(e);
            assertEquals(e.getSemanticErrorCode(), SemanticErrorCode.TABLE_PRIMARY_KEY_MUST_ONE);
        }
    }

    @Test
    public void testCheckWithPartitionBy() {
        List<ColumnDefinition> columnDefinitions = constructorTypeStatement();
        PartitionedBy partitionedBy = new PartitionedBy(columnDefinitions);
        CreateDimTable build = CreateDimTable.builder().tableName(
                QualifiedName.of("a.b")).columns(columnDefinitions).partition(partitionedBy)
            .build();
        assertNotNull(build);

    }

    @Test
    public void testCheckWithPartitionByNormal() {
        List<ColumnDefinition> columnDefinitions = constructorTypeStatement();
        PartitionedBy partitionedBy = new PartitionedBy(columnDefinitions);
        ImmutableList<BaseConstraint> c1 = ImmutableList.of(
            new DimConstraint(new Identifier("c1"), ImmutableList.of(COL_1), QualifiedName.of("a.b"),
                ImmutableList.of(COL_1)));
        CreateTable.builder().tableName(
            QualifiedName.of("a.b")).constraints(c1).partition(partitionedBy).build();
    }

    @Test(expected = SemanticException.class)
    public void testCheckWithPartitionByNotExist() {
        List<ColumnDefinition> columnDefinitions = constructorTypeStatement();
        PartitionedBy partitionedBy = new PartitionedBy(columnDefinitions);
        ImmutableList<BaseConstraint> c1 = ImmutableList.of(
            new DimConstraint(new Identifier("c1"), ImmutableList.of(new Identifier("NOT_EXISTS")),
                QualifiedName.of("a.b"),
                ImmutableList.of(COL_1)));
        CreateTable.builder().tableName(
            QualifiedName.of("a.b")).constraints(c1).partition(partitionedBy).build();
    }

    private List<BaseConstraint> constructConstraintMultiPrimary() {
        PrimaryConstraint primaryConstraint = new PrimaryConstraint(new Identifier("abc"),
            ImmutableList.of(new Identifier("abc")));
        PrimaryConstraint primaryConstraint1 = new PrimaryConstraint(new Identifier("e"),
            ImmutableList.of(new Identifier("abc")));
        return ImmutableList.of(primaryConstraint1, primaryConstraint);
    }

    @Test
    public void testCheckColNameSizeNotEquals() {
        List<ColumnDefinition> list = constructorTypeStatement();
        List<BaseConstraint> baseConstraints = constructorDimStatement();
        CreateDimTable createTableStatement = CreateDimTable.builder().tableName(
            QualifiedName.of("1")).columns(list).constraints(baseConstraints).comment(
            new Comment("comment")
        ).properties(
            ImmutableList.of(new Property("key", "value"))
        ).build();

        try {
            createTableSemanticCheck.check(createTableStatement);
        } catch (SemanticException e) {
            assertNotNull(e);
            assertTrue(e.getMessage().contains("size"));
            assertSame(e.getSemanticErrorCode(), SemanticErrorCode.TABLE_CONSTRAINT_REF_SIZE_MUST_EQUAL);
        }
    }

    private List<BaseConstraint> constructorDimStatement() {
        List<Identifier> colNames = Collections.singletonList(COL_1);
        QualifiedName of = QualifiedName.of("a.b");
        DimConstraint dimConstraintStatement = new DimConstraint(new Identifier("1"), colNames, of, colNames);
        return Collections.singletonList(dimConstraintStatement);
    }

    private List<ColumnDefinition> constructorDuplicateStatement() {
        ColumnDefinition columnDefine = ColumnDefinition.builder().colName(new Identifier("col1")).dataType(
            DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).build();
        List<ColumnDefinition> list
            = new ArrayList<>();
        list.add(columnDefine);
        list.add(columnDefine);
        return list;
    }

    private List<BaseConstraint> constructorConstraintStatement() {
        List<BaseConstraint> list = new ArrayList<>();
        List<Identifier> col2 = Collections.singletonList(new Identifier("col1"));
        PrimaryConstraint primaryConstraintStatement = new PrimaryConstraint(new Identifier("abc"), col2);
        list.add(primaryConstraintStatement);
        return list;
    }

    private List<ColumnDefinition> constructorTypeStatement() {
        ColumnDefinition columnDefine = ColumnDefinition.builder().colName(COL_1).dataType(
            DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).build();
        return Collections.singletonList(columnDefine);
    }
}
