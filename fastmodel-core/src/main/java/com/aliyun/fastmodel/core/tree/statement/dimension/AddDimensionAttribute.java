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

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.BaseOperatorStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.aliyun.fastmodel.core.tree.statement.dimension.attribute.DimensionAttribute;
import lombok.Getter;

/**
 * 添加维度字段
 *
 * @author panguanjing
 * @date 2021/10/1
 */
@Getter
public class AddDimensionAttribute extends BaseOperatorStatement {

    private final List<DimensionAttribute> dimensionAttributes;

    public AddDimensionAttribute(QualifiedName qualifiedName,
                                 List<DimensionAttribute> dimensionAttributes) {
        super(qualifiedName);
        this.dimensionAttributes = dimensionAttributes;
        setStatementType(StatementType.DIMENSION);
    }

    public AddDimensionAttribute(NodeLocation nodeLocation, String origin,
                                 QualifiedName qualifiedName,
                                 List<DimensionAttribute> dimensionAttributes) {
        super(nodeLocation, origin, qualifiedName);
        this.dimensionAttributes = dimensionAttributes;
        setStatementType(StatementType.DIMENSION);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAddDimensionField(this, context);
    }
}
