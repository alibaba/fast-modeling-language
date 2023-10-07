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

package com.aliyun.fastmodel.core.tree.statement.dimension.attribute;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AbstractFmlNode;
import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * 维度字段
 *
 * @author panguanjing
 * @date 2021/10/1
 */
@Getter
public class DimensionAttribute extends AbstractFmlNode {

    private final Identifier attrName;

    private final AliasedName aliasedName;

    private final AttributeCategory category;

    private final Boolean primary;

    private final Comment comment;

    private final List<Property> properties;

    protected DimensionAttribute(DimensionAttributeBuilder dimensionFieldBuilder) {
        attrName = dimensionFieldBuilder.attrName;
        aliasedName = dimensionFieldBuilder.aliasedName;
        comment = dimensionFieldBuilder.comment;
        properties = dimensionFieldBuilder.properties;
        category = dimensionFieldBuilder.category;
        primary = dimensionFieldBuilder.primary;
    }

    public static DimensionAttributeBuilder builder() {
        return new DimensionAttributeBuilder();
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDimensionField(this, context);
    }

    public String getAliasValue() {
        if (aliasedName == null) {
            return StringUtils.EMPTY;
        }
        return aliasedName.getName();
    }

    public String getCommentValue() {
        if (comment == null) {
            return StringUtils.EMPTY;
        }
        return comment.getComment();
    }

    public static final class DimensionAttributeBuilder {

        private Identifier attrName;

        private AliasedName aliasedName;

        private Comment comment;

        private List<Property> properties;

        private AttributeCategory category;

        private Boolean primary;

        public DimensionAttributeBuilder attrName(Identifier attrName) {
            this.attrName = attrName;
            return this;
        }

        public DimensionAttributeBuilder category(AttributeCategory category) {
            this.category = category;
            return this;
        }

        public DimensionAttributeBuilder aliasName(AliasedName aliasedName) {
            this.aliasedName = aliasedName;
            return this;
        }

        public DimensionAttributeBuilder properties(List<Property> properties) {
            this.properties = properties;
            return this;
        }

        public DimensionAttributeBuilder comment(Comment comment) {
            this.comment = comment;
            return this;
        }

        public DimensionAttributeBuilder primary(Boolean primary) {
            this.primary = primary;
            return this;
        }

        public DimensionAttribute build() {
            return new DimensionAttribute(this);
        }
    }
}
