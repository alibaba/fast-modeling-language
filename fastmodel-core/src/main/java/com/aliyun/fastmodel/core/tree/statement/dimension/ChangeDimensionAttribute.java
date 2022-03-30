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

package com.aliyun.fastmodel.core.tree.statement.dimension;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.BaseOperatorStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.aliyun.fastmodel.core.tree.statement.dimension.attribute.DimensionAttribute;
import lombok.Getter;

/**
 * 修改维度字段
 *
 * @author panguanjing
 * @date 2021/10/1
 */
@Getter
public class ChangeDimensionAttribute extends BaseOperatorStatement {
    private final DimensionAttribute dimensionAttribute;

    private final Identifier needChangeAttr;

    public ChangeDimensionAttribute(QualifiedName qualifiedName,
                                    Identifier needChangeAttr,
                                    DimensionAttribute dimensionAttribute) {
        super(qualifiedName);
        this.needChangeAttr = needChangeAttr;
        this.dimensionAttribute = dimensionAttribute;
        setStatementType(StatementType.DIMENSION);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitChangeDimensionField(this, context);
    }
}
