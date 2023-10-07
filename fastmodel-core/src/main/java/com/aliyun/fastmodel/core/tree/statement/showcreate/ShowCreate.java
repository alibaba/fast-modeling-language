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

package com.aliyun.fastmodel.core.tree.statement.showcreate;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.BaseQueryStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.ShowType;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * 显示ShowCreate
 *
 * @author panguanjing
 * @date 2020/11/30
 */
@EqualsAndHashCode(callSuper = true)
@Getter
public class ShowCreate extends BaseQueryStatement {

    /**
     * 显示什么数据类型
     */
    private final ShowType showType;

    private final String identifier;

    private final Output output;

    public ShowCreate(QualifiedName qualifiedName,
                      ShowType showType) {
        this(qualifiedName, showType, null);
    }

    public ShowCreate(QualifiedName qualifiedName, ShowType showType, Output output) {
        super(qualifiedName.getFirstIdentifierIfSizeOverOne());
        this.showType = showType;
        identifier = qualifiedName.getSuffixPath();
        this.output = output;
        setStatementType(StatementType.SHOW_CREATE);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowCreate(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }
}
