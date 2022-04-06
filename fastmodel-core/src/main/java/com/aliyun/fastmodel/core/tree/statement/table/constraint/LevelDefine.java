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

package com.aliyun.fastmodel.core.tree.statement.table.constraint;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AbstractFmlNode;
import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * 层级的定义属性
 *
 * @author panguanjing
 * @date 2020/9/17
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class LevelDefine extends AbstractFmlNode {
    /**
     * 层级的列名
     */
    private final Identifier levelColName;

    /**
     * 层级的属性的字段列表
     */
    private final List<Identifier> levelPropColNames;

    public LevelDefine(Identifier levelColName,
                       List<Identifier> levelPropColNames) {
        this(null, levelColName, levelPropColNames);
    }

    public LevelDefine(NodeLocation location,
                       Identifier levelColName,
                       List<Identifier> levelPropColNames) {
        super(location);
        this.levelColName = levelColName;
        this.levelPropColNames = levelPropColNames;
    }

    public boolean isLevelPropColNameEmpty() {
        return levelPropColNames == null || levelPropColNames.isEmpty();
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.copyOf(levelPropColNames);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitLevelDefine(this, context);
    }
}
