/*
 * Copyright [2024] [name of copyright owner]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import lombok.Data;

/**
 * hash sub partition element
 *
 * @author panguanjing
 * @date 2024/2/7
 */
@Data
public class SubHashPartitionElement extends BaseSubPartitionElement {
    private final QualifiedName qualifiedName;

    private final Property property;

    public SubHashPartitionElement(QualifiedName qualifiedName, Property property) {
        this.qualifiedName = qualifiedName;
        this.property = property;
    }

    @Override
    public List<? extends Node> getChildren() {
        Builder<Node> builder = ImmutableList.builder();
        if (qualifiedName != null) {
            builder.add(qualifiedName);
        }
        if (property != null) {
            builder.add(property);
        }
        return builder.build();
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        ExtensionAstVisitor<R, C> astVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return astVisitor.visitHashSubPartitionElement(this, context);
    }
}
