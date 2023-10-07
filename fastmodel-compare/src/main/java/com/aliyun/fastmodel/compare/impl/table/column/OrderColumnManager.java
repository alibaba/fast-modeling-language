/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.compare.impl.table.column;

import java.util.List;
import java.util.Objects;

import com.aliyun.fastmodel.compare.util.CompareUtil;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.SetColumnOrder;
import com.google.common.base.Preconditions;
import lombok.Getter;
import org.apache.commons.lang3.BooleanUtils;

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
    @Getter
    private final List<ColumnDefinition> list;

    public OrderColumnManager(QualifiedName tableName, List<ColumnDefinition> list) {
        Preconditions.checkNotNull(tableName, "tableName can't be null");
        Preconditions.checkNotNull(list, "column can't be null");
        this.tableName = tableName;
        this.list = list;
    }

    public int getPosition(Identifier col) {
        return CompareUtil.getPosition(list, col);
    }

    /**
     * 更新节点信息
     *
     * @param setColumnOrder
     */
    public void apply(SetColumnOrder setColumnOrder) {
        Identifier changeColumn = setColumnOrder.getOldColName();
        Boolean first = setColumnOrder.getFirst();
        int position = getPosition(changeColumn);
        if (BooleanUtils.isTrue(first)) {
            //如果是first,那么移动到首位
            ColumnDefinition remove = list.remove(position);
            list.add(0, remove);
        } else {
            Identifier beforeColumn = setColumnOrder.getBeforeColName();
            ColumnDefinition remove = list.remove(position);
            int beforePosition = getPosition(beforeColumn);
            list.add(beforePosition + 1, remove);
        }
    }


    /**
     * 根据位置获取排序的列
     *
     * @param position
     * @return
     */
    public OrderColumn getBeforeColumn(Integer position) {
        int index = position - 1;
        if (index < 0) {
            return null;
        }
        return new OrderColumn(index, list.get(index));
    }

    /**
     * 根据列名获取排序的列
     *
     * @param colName
     * @return
     */
    public OrderColumn getBeforeColumn(Identifier colName) {
        int position = getPosition(colName);
        return getBeforeColumn(position);
    }

    public OrderColumn getColumn(Integer position) {
        return new OrderColumn(position, list.get(position));
    }
    /**
     * 根据列名获取排序的列
     *
     * @param colName
     * @return
     */
    public OrderColumn getAfterColumn(Identifier colName) {
        int position = getPosition(colName);
        return getAfterColumn(position);
    }

    public OrderColumn getAfterColumn(Integer position) {
        int index = position + 1;
        if (index >= list.size()) {
            return null;
        }
        return new OrderColumn(index, list.get(index));
    }

    public OrderColumn getOrderColumn(Identifier column) {
        int position = getPosition(column);
        if (position < 0) {
            return null;
        }
        return new OrderColumn(position, list.get(position));
    }

    /**
     * 比对
     *
     * @param changeColumn
     * @param afterColumnManager
     * @return
     */
    public SetColumnOrder compare(Identifier changeColumn, OrderColumnManager afterColumnManager) {
        OrderColumn column = getOrderColumn(changeColumn);
        if (column == null) {
            return null;
        }
        //原来的位置
        OrderColumn afterOrderColumn = afterColumnManager.getOrderColumn(changeColumn);
        if (afterOrderColumn == null) {
            return null;
        }
        Integer afterOrderColumnPosition = afterOrderColumn.getPosition();
        //如果是第一个，那么直接将位置移动到第一个, 第一个不进行返回
        if (afterOrderColumnPosition == 0) {
            if (Objects.equals(column.getPosition(), afterOrderColumnPosition)) {
                return null;
            }
            return new SetColumnOrder(tableName, changeColumn,
                afterOrderColumn.getColumnDefinition().getColName(),
                afterOrderColumn.getColumnDefinition().getDataType(),
                null, true
            );
        } else {
            OrderColumn orderColumn = afterColumnManager.getBeforeColumn(afterOrderColumnPosition);
            if (orderColumn == null) {
                //如果没有找到，那么返回空
                return null;
            }
            OrderColumn beforeColumn = getBeforeColumn(changeColumn);
            //如果前面是空的, 那么移动到
            if (beforeColumn == null) {
                return new SetColumnOrder(tableName, changeColumn,
                    afterOrderColumn.getColumnDefinition().getColName(),
                    afterOrderColumn.getColumnDefinition().getDataType(),
                    orderColumn.getColumnDefinition().getColName(), false);
            } else {
                Identifier colName = beforeColumn.getColumnDefinition().getColName();
                //如果前面和现在是相同的，那么不进行处理
                if (Objects.equals(orderColumn.getColumnDefinition().getColName(), colName)) {
                    return null;
                } else {
                    return new SetColumnOrder(tableName, changeColumn,
                        afterOrderColumn.getColumnDefinition().getColName(),
                        afterOrderColumn.getColumnDefinition().getDataType(),
                        orderColumn.getColumnDefinition().getColName(), false);
                }
            }
        }
    }

}
