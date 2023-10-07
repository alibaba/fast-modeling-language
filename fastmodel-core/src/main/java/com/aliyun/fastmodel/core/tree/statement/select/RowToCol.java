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

package com.aliyun.fastmodel.core.tree.statement.select;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * 行转列语句
 *
 * @author panguanjing
 * @date 2020/10/23
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class RowToCol extends BaseStatement {

    private final Query query;

    public RowToCol(Query query) {
        super(null, query.getOrigin());
        Preconditions.checkNotNull(query);
        Preconditions.checkNotNull(query.getQueryBody());
        this.query = query;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(query);
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return visitor.visitRowToCol(this, context);
    }
}
