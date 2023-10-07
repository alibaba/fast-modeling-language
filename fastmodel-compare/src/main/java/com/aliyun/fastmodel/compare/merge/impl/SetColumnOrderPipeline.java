/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.compare.merge.impl;

import java.util.List;
import java.util.Objects;

import com.aliyun.fastmodel.compare.impl.table.column.OrderColumn;
import com.aliyun.fastmodel.compare.merge.Pipeline;
import com.aliyun.fastmodel.compare.merge.PipelineComponent;
import com.aliyun.fastmodel.compare.util.CompareUtil;
import com.aliyun.fastmodel.compare.util.MergeTableUtil;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable.TableBuilder;
import com.aliyun.fastmodel.core.tree.statement.table.SetColumnOrder;
import com.google.auto.service.AutoService;
import com.google.common.collect.Lists;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/10/9
 */
@PipelineComponent(SetColumnOrder.class)
@AutoService(Pipeline.class)
public class SetColumnOrderPipeline implements Pipeline<SetColumnOrder, CreateTable> {

    @Override
    public CreateTable process(CreateTable input, SetColumnOrder baseStatement) {
        List<ColumnDefinition> columnDefines = Lists.newArrayList(input.getColumnDefines());
        Boolean first = baseStatement.getFirst();
        Identifier oldColName = baseStatement.getOldColName();
        Identifier newColName = baseStatement.getNewColName();
        int changeColumnPosition = getPosition(columnDefines, oldColName);
        //can not find the column
        if (changeColumnPosition < 0) {
            return input;
        }
        ColumnDefinition needChangeColumn = columnDefines.get(changeColumnPosition);
        ColumnDefinition newColumn = getColumnDefinition(newColName, needChangeColumn);
        if (first) {
            columnDefines.remove(needChangeColumn);
            //adjust position
            columnDefines.add(0, newColumn);
        } else {
            Identifier beforeColumn = baseStatement.getBeforeColName();
            ColumnDefinition remove = columnDefines.remove(changeColumnPosition);
            int position = getPosition(columnDefines, beforeColumn);
            columnDefines.add(position + 1, remove);
        }
        TableBuilder builder = MergeTableUtil.getSuppier(input.getTableDetailType().getParent());
        return builder
            .comment(input.getComment())
            .columns(columnDefines)
            .tableName(input.getQualifiedName())
            .detailType(input.getTableDetailType())
            .partition(input.getPartitionedBy())
            .constraints(input.getConstraintStatements())
            .tableIndex(input.getTableIndexList())
            .aliasedName(input.getAliasedName())
            .createOrReplace(input.getCreateOrReplace())
            .properties(input.getProperties())
            .build();
    }

    private static ColumnDefinition getColumnDefinition(Identifier newColName, ColumnDefinition needChangeColumn) {
        ColumnDefinition newColumn = ColumnDefinition
            .builder()
            .colName(newColName)
            .aliasedName(needChangeColumn.getAliasedName())
            .comment(needChangeColumn.getComment())
            .category(needChangeColumn.getCategory())
            .dataType(needChangeColumn.getDataType())
            .defaultValue(needChangeColumn.getDefaultValue())
            .notNull(needChangeColumn.getNotNull())
            .primary(needChangeColumn.getPrimary())
            .properties(needChangeColumn.getColumnProperties())
            .refDimension(needChangeColumn.getRefDimension())
            .refIndicators(needChangeColumn.getRefIndicators())
            .build();
        return newColumn;
    }

    private int getPosition(List<ColumnDefinition> columnDefines, Identifier identifier) {
        return CompareUtil.getPosition(columnDefines, identifier);
    }
}
