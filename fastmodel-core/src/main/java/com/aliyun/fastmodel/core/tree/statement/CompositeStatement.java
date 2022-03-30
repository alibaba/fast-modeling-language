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

package com.aliyun.fastmodel.core.tree.statement;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * 复合的statement
 *
 * @author panguanjing
 * @date 2020/10/19
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class CompositeStatement extends BaseStatement {
    private final List<BaseStatement> statements;

    public CompositeStatement(List<BaseStatement> statements) {
        this(null, null, statements);
    }

    public CompositeStatement(NodeLocation nodeLocation, String origin, List<BaseStatement> statements) {
        super(nodeLocation, origin);
        this.statements = statements;
        setStatementType(StatementType.COMPOSITE);
    }

    @Override
    public List<BaseStatement> getChildren() {
        return statements;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCompositeStatement(this, context);
    }

}
