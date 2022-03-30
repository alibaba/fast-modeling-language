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

package com.aliyun.fastmodel.transform.mysql.format;

import java.util.Arrays;
import java.util.List;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.UniqueConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.index.IndexColumnName;
import com.aliyun.fastmodel.core.tree.statement.table.index.TableIndex;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.transform.mysql.context.MysqlTransformContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * MysqlVisitorTest
 *
 * @author panguanjing
 * @date 2021/9/1
 */
public class MysqlVisitorTest {

    @Test
    public void testVisitPrimary() {
        MysqlVisitor mysqlVisitor = new MysqlVisitor(MysqlTransformContext.builder().build());
        ImmutableList<ColumnDefinition> c1 = ImmutableList.of(
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                .build());
        List<BaseConstraint> constraints =
            ImmutableList.of(
                new PrimaryConstraint(new Identifier("c1"), ImmutableList.of(new Identifier("c1")))
            );
        CreateTable createTable = CreateTable.builder()
            .tableName(QualifiedName.of("dim_shop"))
            .columns(c1)
            .constraints(constraints)
            .build();

        mysqlVisitor.visitCreateTable(createTable, 0);
        String s = mysqlVisitor.getBuilder().toString();
        assertEquals(s, "CREATE TABLE dim_shop\n"
            + "(\n"
            + "   c1 BIGINT,\n"
            + "   PRIMARY KEY(c1)\n"
            + ")");
    }

    @Test
    public void visitCreateTable() {
        MysqlVisitor mysqlVisitor = new MysqlVisitor(MysqlTransformContext.builder().build());
        CreateTable node =
            CreateTable.builder()
                .columns(ImmutableList.of(
                    ColumnDefinition.builder()
                        .colName(new Identifier("c1"))
                        .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                        .build()
                )).tableName(QualifiedName.of("dim_shop"))
                .constraints(
                    Lists.newArrayList(
                        new PrimaryConstraint(IdentifierUtil.sysIdentifier(), Arrays.asList(new Identifier("c1"))),
                        new UniqueConstraint(IdentifierUtil.sysIdentifier(), Arrays.asList(new Identifier("c1")), true))

                )
                .tableIndex(
                    ImmutableList.of(
                        new TableIndex(new Identifier("idx_name"), Lists.newArrayList(
                            new IndexColumnName(new Identifier("c1"), null, null)
                        ), null)
                    )
                ).build();
        Boolean aBoolean = mysqlVisitor.visitCreateTable(node, 0);
        String s = mysqlVisitor.getBuilder().toString();
        assertEquals(s, "CREATE TABLE dim_shop\n"
            + "(\n"
            + "   c1 BIGINT,\n"
            + "   PRIMARY KEY(c1),\n"
            + "   UNIQUE KEY (c1),\n"
            + "   INDEX idx_name (c1)\n)");
    }
}