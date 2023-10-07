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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.compare.impl.table.column.OrderColumn;
import com.aliyun.fastmodel.compare.impl.table.column.OrderColumnManager;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.SetColumnOrder;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
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
    public void testGetAfter() {
        List<ColumnDefinition> list = initList("c", "b", "a");
        SetColumnOrder compare = orderColumnManager.compare(new Identifier("b"), new OrderColumnManager(QualifiedName.of("abc"),
            list));
        assertEquals(compare.getBeforeColName(), new Identifier("c"));
    }

    @Test
    public void testGetAfter2() {
        List<ColumnDefinition> before = initList("h", "g", "f", "e", "d", "c", "b", "a");
        orderColumnManager = new OrderColumnManager(QualifiedName.of("dim_shop"), before);
        List<ColumnDefinition> after = initList("h", "g", "f", "d", "c", "e", "b", "a");
        SetColumnOrder compare = orderColumnManager.compare(new Identifier("f"), new OrderColumnManager(QualifiedName.of("abc"),
            after));
        assertNull(compare);
    }

    @Test
    public void getOrder() {
        List<ColumnDefinition> list = initAfterList();
        OrderColumnManager afterColumnManager = new OrderColumnManager(QualifiedName.of("dim_shop"), list);
        SetColumnOrder orderColumn = orderColumnManager.compare(new Identifier("a"), afterColumnManager);
        assertEquals(orderColumn.toString(), "ALTER TABLE dim_shop CHANGE COLUMN a a BIGINT AFTER b");
    }

    @Test
    public void testApply() {
        List<ColumnDefinition> list = initAfterList();
        QualifiedName dimShop = QualifiedName.of("dim_shop");
        OrderColumnManager afterColumnManager = new OrderColumnManager(dimShop, list);
        SetColumnOrder order = new SetColumnOrder(
            dimShop, new Identifier("b"),new Identifier("b"), DataTypeUtil.simpleType(DataTypeEnums.BIGINT),
            new Identifier("a"), false
        );
        afterColumnManager.apply(order);
        List<ColumnDefinition> list1 = afterColumnManager.getList();
        String collect = list1.stream().map(x -> {
            return x.getColName().getValue();
        }).collect(Collectors.joining(","));
        assertEquals(collect, "c,a,b");
    }

    private List<ColumnDefinition> initAfterList() {
        return initList("c", "b", "a");
    }

    private List<ColumnDefinition> initList(String... b) {
        List<ColumnDefinition> list = new ArrayList<>();
        for (String bb : b) {
            ColumnDefinition c1 = ColumnDefinition.builder()
                .colName(new Identifier(bb))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                .build();
            list.add(c1);
        }
        return list;
    }
}