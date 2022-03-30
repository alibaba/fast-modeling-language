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
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.UniqueConstraint;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * ConstraintCompareTest
 *
 * @author panguanjing
 * @date 2021/8/30
 */
public class ConstraintCompareTest {

    ConstraintCompare constraintCompare = new ConstraintCompare();

    @Test
    public void compareTableElement() {
        CreateTable before = CreateTable.builder()
            .columns(
                ImmutableList.of(
                    ColumnDefinition.builder()
                        .colName(new Identifier("a"))
                        .dataType(DataTypeUtil.simpleType(DataTypeEnums.STRING))
                        .build()
                )
            )
            .constraints(
                ImmutableList.of(
                    new UniqueConstraint(IdentifierUtil.sysIdentifier(), ImmutableList.of(new Identifier("a")), true)
                )
            )
            .detailType(TableDetailType.NORMAL_DIM)
            .tableName(QualifiedName.of("dim_shop"))
            .build();

        CreateTable after = CreateTable.builder()
            .columns(
                ImmutableList.of(
                    ColumnDefinition.builder()
                        .colName(new Identifier("a"))
                        .dataType(DataTypeUtil.simpleType(DataTypeEnums.STRING))
                        .build()
                )
            )
            .constraints(
                ImmutableList.of(
                    new UniqueConstraint(IdentifierUtil.sysIdentifier(), ImmutableList.of(new Identifier("a")), true)
                )
            )
            .detailType(TableDetailType.NORMAL_DIM)
            .tableName(QualifiedName.of("dim_shop"))
            .build();
        List<BaseStatement> baseStatementList = constraintCompare.compareTableElement(before, after);
        assertEquals(0, baseStatementList.size());
    }
}