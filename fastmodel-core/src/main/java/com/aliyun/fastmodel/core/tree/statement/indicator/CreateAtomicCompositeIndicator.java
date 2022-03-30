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
import com.aliyun.fastmodel.core.tree.statement.constants.IndicatorType;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.google.common.base.Preconditions;
import lombok.Getter;

/**
 * 创建复合原子指标
 * <p>
 * DSL举例
 * <pre>
 *        CREATE INDICATOR  ind_code bigint WITH IDCPROPERTIES('type' = 'ATOMIC_COMPOSITE') AS
 *  (case when a=1 then price when a=2 then 1 end);
 *     </pre>
 *
 * @author panguanjing
 * @date 2020/9/10
 */
@Getter
public class CreateAtomicCompositeIndicator extends CreateIndicator {

    public CreateAtomicCompositeIndicator(CreateElement element,
                                          BaseDataType dataType,
                                          BaseExpression indicatorExpr) {
        super(element, dataType, indicatorExpr, IndicatorType.ATOMIC_COMPOSITE);
        Preconditions.checkNotNull(dataType, "atomic indicator dataType must be set!");
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.createAtomicCompositeIndicator(this, context);
    }
}
