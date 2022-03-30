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

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * 基本的单元的内容处理, 语句中需要指定业务板块才能执行
 *
 * @author panguanjing
 * @date 2020/12/15
 */
@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
public abstract class BaseQueryStatement extends BaseStatement {

    /**
     * 语句必须在特定的业务板块中执行
     */
    private Identifier baseUnit;

    public BaseQueryStatement(Identifier baseUnit) {
        this(null, null, baseUnit);
    }

    public BaseQueryStatement(NodeLocation nodeLocation, Identifier baseUnit) {
        this(nodeLocation, null, baseUnit);
    }

    public BaseQueryStatement(NodeLocation location, String origin,
                              Identifier baseUnit) {
        super(location, origin);
        this.baseUnit = baseUnit;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitBaseQueryStatement(this, context);
    }
}

