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

package com.aliyun.fastmodel.core.tree.statement.timeperiod;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.similar.BetweenPredicate;
import com.aliyun.fastmodel.core.tree.statement.BaseSetProperties;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * 修改时间周期属性
 * <p>
 * DSL举例
 * <pre>
 *       ALTER TIMEPERIOD unit.period_name SET
 *        AS BETWEEN sub_day(${bizdate}, 2) AND add_day(${bizdate}, 1);
 *
 *        --设置属性
 *      ALTER TIMEPERIOD unit.period_name SET TPPROPERTIES('key'='value');
 *     </pre>
 *
 * @author panguanjing
 * @date 2020/11/13
 */
@Getter
@EqualsAndHashCode(callSuper = true)
public class SetTimePeriodProperties extends BaseSetProperties {

    private final BetweenPredicate betweenPredicate;

    public SetTimePeriodProperties(QualifiedName qualifiedName,
                                   List<Property> propertyList,
                                   BetweenPredicate betweenPredicate) {
        super(qualifiedName, propertyList);
        this.betweenPredicate = betweenPredicate;
        this.setStatementType(StatementType.TIME_PERIOD);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSetTimePeriodExpression(this, context);
    }
}
