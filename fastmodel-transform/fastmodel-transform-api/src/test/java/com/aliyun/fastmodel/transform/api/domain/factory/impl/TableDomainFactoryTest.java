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

package com.aliyun.fastmodel.transform.api.domain.factory.impl;

import java.util.List;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.constants.TableType;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.transform.api.domain.table.TableDataModel;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/12/19
 */
public class TableDomainFactoryTest {

    TableDomainFactory tableDomainFactory;

    @Before
    public void setUp() throws Exception {
        tableDomainFactory = new TableDomainFactory();
    }

    @Test
    public void create() {
        CreateTable statement = CreateTable.builder()
            .tableName(QualifiedName.of("dim_shop"))
            .detailType(TableDetailType.NORMAL_DIM)
            .build();
        TableDataModel tableDataModel = tableDomainFactory.create(statement);
        String tableType = tableDataModel.getTableType();
        assertEquals(tableType, TableType.DIM.getCode());
    }

    @Test
    public void create_with_partition() {
        List<ColumnDefinition> columnDefinitions = ImmutableList.of(
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .dataType(new GenericDataType("col1"))
                .build()
        );
        CreateTable statement = CreateTable.builder()
            .tableName(QualifiedName.of("dim_shop"))
            .detailType(TableDetailType.NORMAL_DIM)
            .partition(new PartitionedBy(columnDefinitions))
            .build();
        TableDataModel tableDataModel = tableDomainFactory.create(statement);
        String tableType = tableDataModel.getTableType();
        assertEquals(tableType, TableType.DIM.getCode());
        assertEquals(tableDataModel.getCols().size(), 1);
    }
}