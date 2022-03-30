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

package com.aliyun.fastmodel.core.tree.statement.element;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/5/7
 */
@EqualsAndHashCode(callSuper = true)
public class CreateElement extends AbstractNode {

    /**
     * 备注信息，一般用于描述信息内容
     */
    @Getter
    private final Comment comment;

    /**
     * 属性信息
     */
    @Getter
    private final List<Property> properties;

    /**
     * 是否存在
     */
    @Getter
    private final boolean notExists;

    /**
     * 别名信息
     */
    @Getter
    private final AliasedName aliasedName;

    /**
     * qualified Name
     */
    @Getter
    private final QualifiedName qualifiedName;

    @Getter
    private final Boolean createOrReplace;

    protected CreateElement(ElementBuilder elementBuilder) {
        comment = elementBuilder.comment;
        properties = elementBuilder.properties;
        notExists = elementBuilder.notExists;
        aliasedName = elementBuilder.aliasedName;
        qualifiedName = elementBuilder.qualifiedName;
        createOrReplace = elementBuilder.createOrReplace;
    }

    public static ElementBuilder builder() {
        return new ElementBuilder();
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> builder = ImmutableList.builder();
        if (comment != null) {
            builder.add(comment);
        }
        if (properties != null) {
            builder.addAll(properties);
        }
        if (aliasedName != null) {
            builder.add(aliasedName);
        }
        if (qualifiedName != null) {
            builder.add(qualifiedName);
        }
        return builder.build();
    }

    public static class ElementBuilder {

        private Comment comment;

        private List<Property> properties;

        /**
         * 是否存在
         */
        private boolean notExists;

        /**
         * 别名信息
         */
        private AliasedName aliasedName;

        /**
         * qualified Name
         */
        private QualifiedName qualifiedName;

        /**
         * createOrReplace
         */
        private Boolean createOrReplace;

        public ElementBuilder comment(Comment comment) {
            this.comment = comment;
            return this;
        }

        public ElementBuilder properties(List<Property> properties) {
            this.properties = properties;
            return this;
        }

        public ElementBuilder notExists(boolean notExists) {
            this.notExists = notExists;
            return this;
        }

        public ElementBuilder aliasedName(AliasedName aliasedName) {
            this.aliasedName = aliasedName;
            return this;
        }

        public ElementBuilder qualifiedName(QualifiedName qualifiedName) {
            this.qualifiedName = qualifiedName;
            return this;
        }

        public ElementBuilder createOrReplace(Boolean createOrReplace) {
            this.createOrReplace = createOrReplace;
            return this;
        }

        public CreateElement build() {
            return new CreateElement(this);
        }

    }

}
