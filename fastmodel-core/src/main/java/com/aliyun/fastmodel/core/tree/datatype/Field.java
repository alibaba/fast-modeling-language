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

package com.aliyun.fastmodel.core.tree.datatype;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/10/30
 */
@Getter
@Setter
public class Field extends AbstractNode {

    private final Identifier name;

    private final BaseDataType dataType;

    private final Comment comment;

    public Field(NodeLocation location,
                 Identifier name, BaseDataType dataType, Comment comment) {
        super(location);
        Preconditions.checkNotNull(name);
        this.name = name;
        this.dataType = dataType;
        this.comment = comment;
    }

    public Field(Identifier name, BaseDataType dataType, Comment comment) {
        this(null, name, dataType, comment);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRowField(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        Builder<Node> builder = ImmutableList.builder();
        builder.add(name);
        builder.add(dataType);
        return builder.build();
    }
}
