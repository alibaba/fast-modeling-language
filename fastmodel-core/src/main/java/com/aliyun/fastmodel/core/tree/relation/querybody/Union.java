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

package com.aliyun.fastmodel.core.tree.relation.querybody;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseRelation;
import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * define union
 *
 * @author panguanjing
 * @date 2020/10/30
 */
@Getter
@Setter
@EqualsAndHashCode(callSuper = false)
@ToString
public class Union extends SetOperation {

    private final List<BaseRelation> relations;

    public Union(List<BaseRelation> relations, boolean distinct) {
        super(distinct);
        this.relations = relations;
    }

    @Override
    public List<BaseRelation> getRelations() {
        return relations;
    }

    @Override
    public List<? extends Node> getChildren() {
        return relations;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return visitor.visitUnion(this, context);
    }
}
