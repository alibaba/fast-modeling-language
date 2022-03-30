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

package com.aliyun.fastmodel.dqc.check;

import com.aliyun.fastmodel.conveter.dqc.DefaultConvertContext;
import com.aliyun.fastmodel.conveter.dqc.check.DropPrimaryConstraintConverter;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/6/17
 */
@RunWith(MockitoJUnitRunner.class)
public class DropPrimaryConstraintConverterTest {
    DropPrimaryConstraintConverter dropPrimaryConstraintConverter;

    DefaultConvertContext context = new DefaultConvertContext();

    @Before
    public void setUp() throws Exception {
        dropPrimaryConstraintConverter = new DropPrimaryConstraintConverter();
        CreateTable createTable = CreateTable.builder().tableName(QualifiedName.of("dim_shop")).
            constraints(
                ImmutableList.of(new PrimaryConstraint(new Identifier("c1"), ImmutableList.of(
                    new Identifier("pk")
                )))
            ).partition(
                new PartitionedBy(
                    ImmutableList.of(
                        ColumnDefinition.builder().colName(new Identifier("code2")).build()
                    )
                )
            ).columns(ImmutableList.of(ColumnDefinition.builder().colName(new Identifier("pk")).build())).build();
        context.setBeforeStatement(createTable);
        context.setAfterStatement(createTable);
    }

    @Test
    public void testPartitionColumn() {
        DropConstraint source = new DropConstraint(
            QualifiedName.of("dim_shop"),
            new Identifier("c1")
        );
        CreateTable createTable = getCreateTableWithPartition();
        BaseStatement convert = dropPrimaryConstraintConverter.convert(source, context);
        assertEquals(convert.toString(), "ALTER DQC_RULE ON TABLE dim_shop PARTITION (code2='[a-zA-Z0-9_-]*')\n"
            + "ADD (\n"
            + "   CONSTRAINT `字段规则-唯一-(pk)` CHECK(DUPLICATE_COUNT(pk) = 0) NOT ENFORCED DISABLE\n"
            + ")");
    }

    private CreateTable getCreateTableWithPartition() {
        CreateTable createTable = CreateTable.builder().tableName(QualifiedName.of("dim_shop")).columns(
            ImmutableList.of(ColumnDefinition.builder().colName(new Identifier("code1"))
                .dataType(DataTypeUtil.simpleType(
                    DataTypeEnums.BIGINT)).build())
        ).constraints(
            ImmutableList.of(new PrimaryConstraint(new Identifier("pk"), ImmutableList.of(new Identifier("code1"))))
        ).partition(new PartitionedBy(ImmutableList.of(ColumnDefinition.builder().colName(new Identifier("code2"))
            .dataType(DataTypeUtil.simpleType(
                DataTypeEnums.BIGINT)).build()))).build();
        return createTable;
    }

    private CreateTable getCreateTable() {
        CreateTable createTable = CreateTable.builder().tableName(QualifiedName.of("dim_shop")).columns(
            ImmutableList.of(ColumnDefinition.builder().colName(new Identifier("code1"))
                .dataType(DataTypeUtil.simpleType(
                    DataTypeEnums.BIGINT)).build())
        ).constraints(
            ImmutableList.of(new PrimaryConstraint(new Identifier("pk"), ImmutableList.of(new Identifier("code1"))))
        ).build();
        return createTable;
    }

}