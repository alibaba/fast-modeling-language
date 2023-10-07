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

package com.aliyun.fastmodel.core.tree.statement.batch.element;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * 表达命名词典的元素
 *
 * @author panguanjing
 * @date 2021/6/23
 */
@Getter
public class NamingDictElement extends AbstractNode {

    private final Identifier name;

    private final AliasedName aliasedName;

    private final Comment comment;

    private final List<Property> properties;

    public NamingDictElement(Identifier name, AliasedName aliasedName, Comment comment,
                             List<Property> properties) {
        this.name = name;
        this.aliasedName = aliasedName;
        this.comment = comment;
        this.properties = properties;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }
}
