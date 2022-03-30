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

import java.util.List;

import com.aliyun.fastmodel.compare.CompareNodeExecute;
import com.aliyun.fastmodel.compare.CompareStrategy;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDwsTable;
import com.aliyun.fastmodel.core.tree.statement.table.RenameTable;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * CreateDwsTable
 *
 * @author panguanjing
 * @date 2021/3/11
 */
public class CreateDwsTableCompareNodeTest {

    @Test
    public void testRename() {
        GenericDataType genericDataType = new GenericDataType(new Identifier(DataTypeEnums.BIGINT.name()));
        ColumnDefinition a1 = ColumnDefinition.builder().colName(new Identifier("a")).dataType(genericDataType).build();
        ImmutableList<ColumnDefinition> a = ImmutableList
            .of(a1);
        CreateDwsTable createDwsTable = CreateDwsTable.builder().tableName(QualifiedName.of("b")).ifNotExist(true)
            .columns(a).build();

        CreateDwsTable createDwsTable1 = CreateDwsTable.builder().tableName(QualifiedName.of("c")).columns(a)
            .ifNotExist(true).build();
        List<BaseStatement> compare = CompareNodeExecute.getInstance().compare(createDwsTable, createDwsTable1,
            CompareStrategy.INCREMENTAL);
        assertEquals(compare.size(), 1);
        RenameTable renameTable = (RenameTable)compare.get(0);
        assertEquals(renameTable.getNewIdentifier(), "c");
    }
}
