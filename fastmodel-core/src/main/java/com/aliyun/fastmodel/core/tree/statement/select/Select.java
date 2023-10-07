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

package com.aliyun.fastmodel.core.tree.statement.select;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.statement.select.item.SelectItem;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * select xxx as a1, yyyy as a2
 *
 * @author panguanjing
 * @date 2020/10/20
 */
@ToString
@Getter
@Setter
@EqualsAndHashCode(callSuper = false)
public class Select extends AbstractNode {
    /**
     * as 语句
     */
    private List<SelectItem> selectItems;

    private final boolean distinct;

    public Select(NodeLocation location,
                  List<SelectItem> selectItems, boolean distinct) {
        super(location);
        this.selectItems = selectItems;
        this.distinct = distinct;
    }

    public Select(List<SelectItem> selectItems, boolean distinct) {
        this.selectItems = selectItems;
        this.distinct = distinct;
    }

    @Override
    public List<? extends Node> getChildren() {
        return selectItems;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return visitor.visitSelect(this, context);
    }
}
