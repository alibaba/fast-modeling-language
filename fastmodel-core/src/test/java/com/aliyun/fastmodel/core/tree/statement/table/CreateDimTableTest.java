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

package com.aliyun.fastmodel.core.tree.statement.table;

import java.util.List;

import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.DimConstraint;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/2/5
 */
public class CreateDimTableTest {

    @Test
    public void testGetChildren() {
        CreateDimTable createDimTable =
            CreateDimTable.builder()
                .createOrReplace(true)
                .tableName(QualifiedName.of("a.b")).columns(
                ImmutableList.of(
                    ColumnDefinition.builder().colName(new Identifier("a")).dataType(DataTypeUtil.simpleType(
                        DataTypeEnums.BIGINT)).build()
                )
            ).constraints(ImmutableList.of(new DimConstraint(new Identifier("abc"), QualifiedName.of("a.b")))).build();

        List<? extends Node> children = createDimTable.getChildren();
        assertEquals(children.size(), 2);
        assertEquals(createDimTable.getCreateElement().getCreateOrReplace(), true);

    }
}