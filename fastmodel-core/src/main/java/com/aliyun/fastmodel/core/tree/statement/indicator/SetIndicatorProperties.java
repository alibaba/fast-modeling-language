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

package com.aliyun.fastmodel.core.tree.statement.indicator;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.statement.BaseSetProperties;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import lombok.Getter;
import lombok.ToString;

/**
 * 更改指标的表达式语句
 * <p>
 * DSL举例
 * <pre>
 *              ALTER INDICATOR unit.idc1 REFERENCES table1 SET IDCPROPERTIES('key1'='value1') AS sum(a*c)
 *     </pre>
 *
 * @author panguanjing
 * @date 2020/9/7
 */
@Getter
@ToString(callSuper = true)
public class SetIndicatorProperties extends BaseSetProperties {

    /**
     * 最新的指标表达式
     */
    private final BaseExpression expression;

    /**
     * 修改的最新的references，可能为空。
     * 为空的语义是不修改
     */
    private final QualifiedName references;

    /**
     * 指标类型处理
     */
    private final BaseDataType primaryTypeDataType;

    public SetIndicatorProperties(QualifiedName qualifiedName, List<Property> propertyList,
                                  BaseExpression expression, QualifiedName references,
                                  BaseDataType dataType) {
        super(qualifiedName, propertyList);
        this.expression = expression;
        this.references = references;
        this.primaryTypeDataType = dataType;
        this.setStatementType(StatementType.INDICATOR);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSetIndicatorProperties(this, context);
    }

}



