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

package com.aliyun.fastmodel.core.tree.statement.materialize;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.BaseCreate;
import com.aliyun.fastmodel.core.tree.statement.constants.MaterializedType;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * 创建物化的语句
 * <p>
 * DSL举例
 * <pre>
 *         CREATE MATERIALIZED VIEW  unit.materialized REFERENCES (abc) COMMENT 'comment' ENGINE odps WITH
 *         ENGINEPROPERTIES('partition'='ds')
 * </pre>
 *
 * @author panguanjing
 * @date 2020/9/21
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class CreateMaterialize extends BaseCreate {
    /**
     * 引用的操作
     */
    private final List<QualifiedName> references;

    /**
     * engine
     */
    private final Identifier engine;

    /**
     * 物化类型
     */
    private final MaterializedType materializedType;

    public CreateMaterialize(CreateElement element,
                             List<QualifiedName> references,
                             Identifier engine,
                             MaterializedType materializedType) {
        super(element);
        this.references = references;
        this.engine = engine;
        this.materializedType = materializedType;
        setStatementType(StatementType.MATERIALIZE);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateMaterialize(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.copyOf(references);
    }
}
