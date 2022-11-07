/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.compare.impl.table;

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
