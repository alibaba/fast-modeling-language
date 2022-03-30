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

package com.aliyun.fastmodel.compare.table.column;

import java.util.List;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.SetColumnOrder;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * 对排序列的管理对象
 *
 * @author panguanjing
 * @date 2022/1/26
 */
public class OrderColumnManagerTest {
    OrderColumnManager orderColumnManager;

    @Before
    public void setUp() throws Exception {
        List<ColumnDefinition> list = initList("a", "b", "c");
        orderColumnManager = new OrderColumnManager(QualifiedName.of("dim_shop"), list);
    }

    @Test
    public void testGetColumn() {
        OrderColumn column = orderColumnManager.getColumn(0);
        assertEquals(column.getPosition(), new Integer(0));
        ColumnDefinition columnName = column.getColumnDefinition();
        assertEquals(columnName.getColName(), new Identifier("a"));

        column = orderColumnManager.getColumn(1);
        columnName = column.getColumnDefinition();
        assertEquals(columnName.getColName(), new Identifier("b"));
    }

    @Test
    public void testGetPosition() {
        OrderColumn beforeColumn = orderColumnManager.getBeforeColumn(1);
        assertEquals(beforeColumn.getPosition(), new Integer(0));
    }

    @Test
    public void testGetPositionByColumn() {
        OrderColumn orderColumn = orderColumnManager.getBeforeColumn(new Identifier("a"));
        assertNull(orderColumn);
        orderColumn = orderColumnManager.getBeforeColumn(new Identifier("b"));
        assertEquals(orderColumn.getColumnDefinition().getColName(), new Identifier("a"));
    }

    @Test
    public void testGetAfterPosition() {
        OrderColumn orderColumn = orderColumnManager.getAfterColumn(0);
        assertEquals(orderColumn.getPosition(), new Integer(1));
        assertEquals(orderColumn.getColumnDefinition().getColName(), new Identifier("b"));
    }

    @Test
    public void testGetAfterPositionByColumn() {
        OrderColumn orderColumn = orderColumnManager.getAfterColumn(new Identifier("a"));
        assertEquals(orderColumn.getPosition(), new Integer(1));
        assertEquals(orderColumn.getColumnDefinition().getColName(), new Identifier("b"));
    }

    @Test
    public void getOrder() {
        List<ColumnDefinition> list = initAfterList();
        OrderColumnManager afterColumnManager = new OrderColumnManager(QualifiedName.of("dim_shop"), list);
        SetColumnOrder orderColumn = orderColumnManager.compare(new Identifier("a"), afterColumnManager);
        assertEquals(orderColumn.toString(), "ALTER TABLE dim_shop CHANGE COLUMN a a BIGINT AFTER b");
    }

    private List<ColumnDefinition> initAfterList() {
        return initList("c", "b", "a");
    }

    private List<ColumnDefinition> initList(String a, String b, String c) {
        ColumnDefinition c1 = ColumnDefinition.builder()
            .colName(new Identifier(a))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
            .build();

        ColumnDefinition c2 = ColumnDefinition.builder()
            .colName(new Identifier(b))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
            .build();
        ColumnDefinition c3 = ColumnDefinition.builder()
            .colName(new Identifier(c))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
            .build();
        return ImmutableList.of(c1, c2, c3);
    }
}