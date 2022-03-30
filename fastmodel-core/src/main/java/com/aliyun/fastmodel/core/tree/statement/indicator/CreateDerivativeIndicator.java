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
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.statement.constants.IndicatorType;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.google.common.base.Preconditions;
import lombok.Getter;

/**
 * 创建衍生指标
 * <p>
 * DSL举例
 * <pre>
 *         CREATE INDICATOR demo.shop_sku_1d_pay_price_avg DECIMAL
 *             REFERENCES pay_price_avg
 *             COMMENT '门店商品_近1天_生鲜门店生鲜类目_平均支付金额'
 *             WITH IDCPROPERTIES('type'='derivative',
 *                                'dim'='fact_trade_pay',
 *                                'date_period' = 'week',
 *                                'filter_expr'= 'category=1',
 *                                'filter_name' = 'name')
 *     </pre>
 *
 * @author panguanjing
 * @date 2020/9/10
 */
@Getter
public class CreateDerivativeIndicator extends CreateIndicator {

    private final QualifiedName references;

    public CreateDerivativeIndicator(CreateElement element,
                                     BaseDataType dataType,
                                     BaseExpression indicatorExpr,
                                     QualifiedName references) {
        super(element, dataType, indicatorExpr, IndicatorType.DERIVATIVE);
        Preconditions.checkNotNull(references, "derivative indicator must set reference!");
        this.references = references;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateDerivativeIndicator(this, context);
    }
}
