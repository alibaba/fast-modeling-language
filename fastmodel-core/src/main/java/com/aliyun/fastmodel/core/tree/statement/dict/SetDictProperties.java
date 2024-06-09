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

package com.aliyun.fastmodel.core.tree.statement.dict;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.literal.BaseLiteral;
import com.aliyun.fastmodel.core.tree.statement.BaseSetProperties;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.NotNullConstraint;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * 设置数据字典属性
 * <p>
 * DSL举例
 * <blockquote><pre>
 *     ALTER DICT unit.shopId bigint SET PROPERTIES (
 *     'memo'= '店铺信息'
 * ) CHECK DQC BY (${value} &gt; 10);
 * </pre></blockquote>
 *
 * @author panguanjing
 * @date 2020/11/13
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class SetDictProperties extends BaseSetProperties {

    private final BaseDataType baseDataType;

    private final BaseConstraint baseConstraint;

    private final BaseLiteral defaultValue;

    public SetDictProperties(QualifiedName qualifiedName,
        List<Property> property,
        BaseDataType baseDataType,
        BaseConstraint baseConstraint,
        BaseLiteral defaultValue) {
        super(qualifiedName, property);
        this.baseDataType = baseDataType;
        this.baseConstraint = baseConstraint;
        this.defaultValue = defaultValue;
        setStatementType(StatementType.DICT);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSetDictProperties(this, context);
    }

    public boolean isNotNull() {
        return baseConstraint != null && baseConstraint instanceof NotNullConstraint;
    }
}
