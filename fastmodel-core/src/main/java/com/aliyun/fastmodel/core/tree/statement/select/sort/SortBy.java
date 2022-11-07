/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.core.tree.statement.select.sort;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.statement.select.order.SortItem;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * sort by node
 *
 * @author panguanjing
 * @date 2022/6/27
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class SortBy extends AbstractNode {

    private final List<SortItem> sortItems;

    public SortBy(List<SortItem> sortItems) {this.sortItems = sortItems;}

    @Override
    public List<? extends Node> getChildren() {
        return sortItems;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return visitor.visitSortBy(this, context);
    }
}
