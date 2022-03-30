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

package com.aliyun.fastmodel.core.tree.statement.script;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * ref object
 *
 * @author panguanjing
 * @date 2021/9/14
 */
@Getter
public class RefObject extends AbstractNode {

    private final QualifiedName mainName;

    private final List<Identifier> attrNameList;

    private final Comment comment;

    public RefObject(QualifiedName mainName, List<Identifier> attrNameList,
                     Comment comment) {
        Preconditions.checkNotNull(mainName, "tableName can't be null");
        this.mainName = mainName;
        this.attrNameList = attrNameList;
        this.comment = comment;
    }

    public String getCommentValue() {
        if (comment == null) {
            return null;
        }
        return comment.getComment();
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> builder = ImmutableList.builder();
        builder.add(mainName);
        if (attrNameList != null) {
            builder.addAll(attrNameList);
        }
        return builder.build();
    }
}
