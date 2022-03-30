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
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.statement.BaseCreate;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.aliyun.fastmodel.core.tree.statement.dimension.attribute.DimensionAttribute;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import lombok.Getter;

/**
 * 创建维度
 *
 * @author panguanjing
 * @date 2021/10/1
 */
@Getter
public class CreateDimension extends BaseCreate {

    private final List<DimensionAttribute> dimensionAttributes;

    protected CreateDimension(CreateDimensionBuilder builder) {
        super(builder.createElement);
        dimensionAttributes = builder.dimensionFields;
        setStatementType(StatementType.DIMENSION);
    }

    public static CreateDimensionBuilder builder() {
        return new CreateDimensionBuilder();
    }

    public boolean isAttributeEmpty() {
        return dimensionAttributes == null || dimensionAttributes.isEmpty();
    }

    @Override
    public List<? extends Node> getChildren() {
        Builder<Node> builder = ImmutableList.builder();
        if (dimensionAttributes != null) {
            builder.addAll(dimensionAttributes);
        }
        return builder.build();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateDimension(this, context);
    }

    public static class CreateDimensionBuilder {

        private CreateElement createElement;

        private List<DimensionAttribute> dimensionFields;

        public CreateDimensionBuilder createElement(CreateElement createElement) {
            this.createElement = createElement;
            return this;
        }

        public CreateDimensionBuilder dimensionFields(List<DimensionAttribute> dimensionFields) {
            this.dimensionFields = dimensionFields;
            return this;
        }

        public CreateDimension build() {
            return new CreateDimension(this);
        }
    }
}
