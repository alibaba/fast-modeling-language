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
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.TableElement;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * table index
 *
 * @author panguanjing
 * @date 2021/8/30
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class TableIndex extends TableElement {

    private final Identifier indexName;

    private final List<IndexSortKey> indexColumnNames;

    private final List<Property> properties;

    public TableIndex(Identifier indexName,
        List<IndexSortKey> indexColumnNames,
        List<Property> properties) {
        this.indexName = indexName;
        this.indexColumnNames = indexColumnNames;
        this.properties = properties;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitTableIndex(this, context);
    }
}
