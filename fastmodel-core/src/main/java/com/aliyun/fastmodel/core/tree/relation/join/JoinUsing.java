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

package com.aliyun.fastmodel.core.tree.relation.join;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/11/3
 */
@Getter
@Setter
@ToString
public class JoinUsing extends JoinCriteria {

    private final List<Identifier> columns;

    public JoinUsing(List<Identifier> columns) {
        this.columns = columns;
    }

    @Override
    public List<AbstractNode> getNodes() {
        return ImmutableList.of();
    }
}
