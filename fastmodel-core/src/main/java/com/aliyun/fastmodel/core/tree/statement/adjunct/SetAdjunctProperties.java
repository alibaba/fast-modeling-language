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
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.statement.BaseSetProperties;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import lombok.Getter;

/**
 * 设置修饰词属性
 * <p>
 * DSL举例
 * <pre>
 *         ALTER Adjunct unit.adjunctName SET adjProperties('param1'='value1') AS dim_abc.shop='abc';
 *     </pre>
 *
 * @author panguanjing
 * @date 2020/11/13
 */
@Getter
public class SetAdjunctProperties extends BaseSetProperties {

    private final BaseExpression expression;

    public SetAdjunctProperties(QualifiedName qualifiedName,
                                List<Property> property, BaseExpression expression) {
        super(qualifiedName, property);
        this.expression = expression;
        this.setStatementType(StatementType.ADJUNCT);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.setAdjunctProperties(this, context);
    }
}
