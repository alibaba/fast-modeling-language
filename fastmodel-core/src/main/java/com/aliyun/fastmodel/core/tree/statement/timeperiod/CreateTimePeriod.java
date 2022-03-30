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

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.expr.similar.BetweenPredicate;
import com.aliyun.fastmodel.core.tree.statement.BaseCreate;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * 创建时间周期
 * <p>
 * DSL举例
 * <pre>
 *         CREATE TIMEPERIOD ut.period_name COMMENT '近3天' AS BETWEEN sub_day(${bizdate}, 2) AND add_day(${bizdate}, 1);
 *     </pre>
 *
 * @author panguanjing
 * @date 2020/11/13
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class CreateTimePeriod extends BaseCreate {

    private final BetweenPredicate betweenPredicate;

    public CreateTimePeriod(CreateElement element,
                            BetweenPredicate betweenPredicate) {
        super(element);
        this.betweenPredicate = betweenPredicate;
        setStatementType(StatementType.TIME_PERIOD);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateTimePeriod(this, context);
    }
}
