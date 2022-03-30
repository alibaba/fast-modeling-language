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

package com.aliyun.fastmodel.compare.table;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.statement.table.AddConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * ConstraintCompare
 *
 * @author panguanjing
 * @date 2021/8/30
 */
public class ConstraintCompare implements TableElementCompare {
    @Override
    public List<BaseStatement> compareTableElement(CreateTable before, CreateTable after) {
        List<BaseConstraint> beforeConstraints = before.getConstraintStatements();
        List<BaseConstraint> afterConstraints = after.getConstraintStatements();
        ImmutableList.Builder<BaseStatement> builder = ImmutableList.builder();
        boolean beforeEmpty = beforeConstraints == null || beforeConstraints.isEmpty();
        boolean afterNotEmpty = afterConstraints != null && !afterConstraints.isEmpty();
        if (beforeEmpty && !afterNotEmpty) {
            return ImmutableList.of();
        }
        if (beforeEmpty && afterNotEmpty) {
            List<AddConstraint> collect = afterConstraints.stream().map(
                x -> new AddConstraint(after.getQualifiedName(), x)).collect(Collectors.toList());
            builder.addAll(collect);
            return builder.build();
        }
        if (!afterNotEmpty) {
            List<DropConstraint> dropConstraints = beforeConstraints.stream().map(
                x -> new DropConstraint(after.getQualifiedName(), x.getName(), x.getConstraintType())
            ).collect(Collectors.toList());
            builder.addAll(dropConstraints);
            return builder.build();
        }
        List<BaseConstraint> baseConstraints = Lists.newArrayList(afterConstraints);
        for (BaseConstraint beforeConstraint : beforeConstraints) {
            if (isNotContain(baseConstraints, beforeConstraint)) {
                builder.add(new DropConstraint(after.getQualifiedName(), beforeConstraint.getName(),
                    beforeConstraint.getConstraintType()));
            } else {
                baseConstraints.remove(beforeConstraint);
            }
        }
        List<AddConstraint> list = baseConstraints.stream().map(x -> new AddConstraint(after.getQualifiedName(), x))
            .collect(Collectors.toList());
        builder.addAll(list);
        return builder.build();
    }

    /**
     * 如果没有包含
     *
     * @param baseConstraints
     * @param c
     * @return
     */
    private boolean isNotContain(List<BaseConstraint> baseConstraints, BaseConstraint c) {
        return !baseConstraints.contains(c);
    }
}
