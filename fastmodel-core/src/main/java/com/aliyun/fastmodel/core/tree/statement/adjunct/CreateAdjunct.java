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

package com.aliyun.fastmodel.core.tree.statement.adjunct;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.statement.BaseCreate;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * 创建修饰词语句，用于建模语言中的创建修饰词
 * <p>
 * DSL举例
 * <pre>
 * CREATE ADJUNCT unit.adjunctName comment '业务过程' AS dim_shop.shop_type='2'
 * </pre>
 *
 * @author panguanjing
 * @date 2020/11/13
 */
@EqualsAndHashCode(callSuper = false)
@Getter
public class CreateAdjunct extends BaseCreate {

    private final BaseExpression expression;

    public CreateAdjunct(CreateElement element, BaseExpression expression) {
        super(element);
        this.expression = expression;
        setStatementType(StatementType.ADJUNCT);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateAdjunct(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(expression);
    }
}

