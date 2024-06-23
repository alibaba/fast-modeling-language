/*
 * Copyright (c)  2021. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.core.tree.statement.table.index;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Index ColumnName
 *
 * @author panguanjing
 * @date 2021/8/30
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class IndexColumnName extends IndexSortKey {

    private final Identifier columnName;

    private final LongLiteral columnLength;

    private final SortType sortType;

    public IndexColumnName(Identifier columnName, LongLiteral columnLength,
        SortType sortType) {
        this.columnName = columnName;
        this.columnLength = columnLength;
        this.sortType = sortType;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitIndexColumnName(this, context);
    }
}
