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

package com.aliyun.fastmodel.core.tree.statement.measure.unit;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.statement.BaseCreate;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * 创建度量单位
 * <p>
 * DSL举例
 * <pre>
 *         CREATE MEASUREUNIT unit.cent COMMENT '分'
 * WITH ('group'='money');
 * </pre>
 *
 * @author panguanjing
 * @date 2020/11/13
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class CreateMeasureUnit extends BaseCreate {

    public CreateMeasureUnit(CreateElement element) {
        super(element);
        setStatementType(StatementType.MEASURE_UNIT);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateMeasureUnit(this, context);
    }
}
