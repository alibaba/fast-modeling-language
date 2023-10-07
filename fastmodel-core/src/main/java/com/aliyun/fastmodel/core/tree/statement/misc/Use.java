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

package com.aliyun.fastmodel.core.tree.statement.misc;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.google.common.base.Preconditions;
import lombok.Getter;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/1/29
 */
@Getter
public final class Use extends BaseStatement {

    private final Identifier businessUnit;

    public Use(Identifier businessUnit) {
        this(null, businessUnit);
    }

    public Use(NodeLocation nodeLocation, Identifier businessUnit) {
        super(nodeLocation);
        this.businessUnit = businessUnit;
        Preconditions.checkNotNull(businessUnit, "please select one businessUnit");
        setStatementType(StatementType.USE);
    }

    @Override
    public List<? extends Node> getChildren() {
        return null;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return visitor.visitUse(this, context);
    }
}
