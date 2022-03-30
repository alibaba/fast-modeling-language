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

package com.aliyun.fastmodel.transform.example;

import java.util.List;

import com.aliyun.fastmodel.compare.CompareNodeExecute;
import com.aliyun.fastmodel.compare.CompareStrategy;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * 增加列的字段的case信息
 *
 * @author panguanjing
 * @date 2021/2/24
 */
public class TransformAddConstraintCaseTest extends BaseTransformCaseTest {

    CreateDimTable before = null;

    CreateDimTable after = null;

    @Test
    public void testAddConstraintWithHive() {
        initTableWithPrimary();
        List<BaseStatement> compare = CompareNodeExecute.getInstance().compare(before, after,
            CompareStrategy.INCREMENTAL);
        String collect = starter.transformHive(compare, ";");
        assertEquals("ALTER TABLE dim_shop ADD COLUMNS\n"
            + "(\n"
            + "   c1 BIGINT COMMENT 'a.b'\n"
            + ")", collect);
    }

    private void initTableWithPrimary() {
        before = CreateDimTable.builder().tableName(QualifiedName.of("a.dim_shop")).build();
        List<ColumnDefinition> columns =
            ImmutableList.of(
                ColumnDefinition.builder().colName(new Identifier("c1")).dataType(new GenericDataType(
                    new Identifier(DataTypeEnums.BIGINT.name())
                )).comment(new Comment("a.b")).build()
            );
        after = CreateDimTable.builder().tableName(QualifiedName.of("a.dim_shop")).columns(columns).build();
    }

    @Test
    public void testAddNewFiledNotNull() {
        before = CreateDimTable.builder().tableName(QualifiedName.of("a.dim_shop")).build();
        List<ColumnDefinition> columns =
            ImmutableList.of(
                ColumnDefinition.builder().colName(new Identifier("c1")).dataType(new GenericDataType(
                    new Identifier(DataTypeEnums.BIGINT.name())
                )).comment(new Comment("a.b")).build()
            );
        after = CreateDimTable.builder().tableName(QualifiedName.of("a.dim_shop")).columns(columns).build();
    }
}
