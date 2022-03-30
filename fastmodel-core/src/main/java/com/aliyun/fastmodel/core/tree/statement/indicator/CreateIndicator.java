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

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.statement.BaseCreate;
import com.aliyun.fastmodel.core.tree.statement.constants.IndicatorType;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * 创建指标的语句
 *
 * @author panguanjing
 * @date 2020/9/3
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class CreateIndicator extends BaseCreate {

    /**
     * 指标类型，比如bigint等，只支持基本类型的指标
     */
    private final BaseDataType dataType;
    /**
     * 指标表达式
     */
    private final BaseExpression indicatorExpr;

    /**
     * 指标类型
     * {@link IndicatorType}
     */
    private final IndicatorType indicatorType;

    public CreateIndicator(CreateElement element, BaseDataType dataType,
                           BaseExpression indicatorExpr, IndicatorType indicatorType) {
        super(element);
        this.dataType = dataType;
        this.indicatorExpr = indicatorExpr;
        this.indicatorType = indicatorType;
        setStatementType(StatementType.INDICATOR);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateIndicator(this, context);
    }
}
