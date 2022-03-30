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
import java.util.Map;
import java.util.Objects;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.SetColumnOrder;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * 排序列管理者
 *
 * @author panguanjing
 * @date 2022/1/26
 */
public class OrderColumnManager {

    /**
     * 表名
     */
    private final QualifiedName tableName;
    /**
     * 管理者
     */
    private final List<ColumnDefinition> list;

    /**
     * 保存列名与列的关系的处理
     */
    private final Map<Identifier, OrderColumn> maps;

    /**
     * 保存列位置与列的管理
     */
    private final Map<Integer, OrderColumn> orderColumnMap;

    public OrderColumnManager(QualifiedName tableName, List<ColumnDefinition> list) {
        Preconditions.checkNotNull(tableName, "tableName can't be null");
        Preconditions.checkNotNull(list, "column can't be null");
        this.tableName = tableName;
        this.list = list;
        maps = Maps.newHashMapWithExpectedSize(list.size());
        orderColumnMap = Maps.newHashMapWithExpectedSize(list.size());
        int index = 0;
        for (ColumnDefinition columnDefinition : list) {
            OrderColumn orderColumn = new OrderColumn(index, columnDefinition);
            maps.put(columnDefinition.getColName(), orderColumn);
            orderColumnMap.put(index, orderColumn);
            index++;
        }
    }

    /**
     * 根据位置获取当前列的内容
     *
     * @param position
     * @return
     */
    public OrderColumn getColumn(Integer position) {
        return orderColumnMap.get(position);
    }

    /**
     * 根据位置获取排序的列
     *
     * @param position
     * @return
     */
    public OrderColumn getBeforeColumn(Integer position) {
        int index = position - 1;
        return orderColumnMap.get(index);
    }

    /**
     * 根据列名获取排序的列
     *
     * @param colName
     * @return
     */
    public OrderColumn getBeforeColumn(Identifier colName) {
        OrderColumn orderColumn = maps.get(colName);
        return getBeforeColumn(orderColumn.getPosition());
    }

    /**
     * 根据列名获取排序的列
     *
     * @param colName
     * @return
     */
    public OrderColumn getAfterColumn(Identifier colName) {
        OrderColumn orderColumn = maps.get(colName);
        return getAfterColumn(orderColumn.getPosition());
    }

    public OrderColumn getAfterColumn(Integer position) {
        int index = position + 1;
        return orderColumnMap.get(index);
    }

    public OrderColumn getOrderColumn(Identifier column) {
        return maps.get(column);
    }

    public SetColumnOrder compare(Identifier beforeColumn, OrderColumnManager afterColumnManager) {
        OrderColumn column = getOrderColumn(beforeColumn);
        if (column == null) {
            return null;
        }
        Integer position = column.getPosition();
        OrderColumn afterOrderColumn = afterColumnManager.getOrderColumn(beforeColumn);
        if (afterOrderColumn == null) {
            return null;
        }
        Integer afterOrderColumnPosition = afterOrderColumn.getPosition();
        if (Objects.equals(position, afterOrderColumnPosition)) {
            return null;
        }
        //如果是第一个，那么直接将位置移动到第一个, 第一个不进行返回
        if (afterOrderColumnPosition == 0) {
            return null;
        } else {
            OrderColumn orderColumn = afterColumnManager.getBeforeColumn(afterOrderColumnPosition);
            if (orderColumn == null) {
                //如果没有找到，那么返回空
                return null;
            }
            return new SetColumnOrder(tableName, beforeColumn, afterOrderColumn.getColumnDefinition().getColName(), afterOrderColumn.getColumnDefinition().getDataType(),
                orderColumn.getColumnDefinition().getColName(), false);
        }
    }

}
