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

package com.aliyun.fastmodel.core.semantic.table;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.exception.SemanticException;
import com.aliyun.fastmodel.core.semantic.SemanticCheck;
import com.aliyun.fastmodel.core.semantic.SemanticErrorCode;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.DimConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.LevelConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * 创建表的语义校验
 *
 * @author panguanjing
 * @date 2020/9/17
 */
public class CreateTableSemanticCheck implements SemanticCheck<CreateTable> {

    @Override
    public void check(CreateTable baseStatement) throws SemanticException {
        List<ColumnDefinition> columnDefines = baseStatement.getColumnDefines();
        List<ColumnDefinition> allColumns = Lists.newArrayList();
        if (columnDefines != null) {
            containSameCol(columnDefines);
            allColumns.addAll(columnDefines);
        }

        if (baseStatement.getPartitionedBy() != null) {
            PartitionedBy partitionedBy = baseStatement.getPartitionedBy();
            if (partitionedBy.isNotEmpty()) {
                List<ColumnDefinition> columnDefinitions = partitionedBy.getColumnDefinitions();
                allColumns.addAll(columnDefinitions);
                containSameCol(columnDefinitions);
            }
        }
        List<BaseConstraint> constraintStatements = baseStatement.getConstraintStatements();
        if (constraintStatements != null) {
            constraintCheck(allColumns, constraintStatements);
        }
    }

    public static void containSameCol(List<ColumnDefinition> statements) {
        Set<Identifier> set = new HashSet<>(4);
        for (ColumnDefinition columnDefine : statements) {
            Identifier colName = columnDefine.getColName();
            if (!set.add(colName)) {
                throw new SemanticException(SemanticErrorCode.TABLE_HAVE_SAME_COLUMN,
                    "Table contains duplicate column names : " + colName);
            }
        }
    }

    private void constraintCheck(
        List<ColumnDefinition> columnDefines,
        List<BaseConstraint> constraintStatements) {

        //primaryKeyConstraint只允许有一个
        Map<String, List<BaseConstraint>> collect = constraintStatements.stream().collect(
            Collectors.groupingBy(x -> x.getClass().getName()));
        List<BaseConstraint> constraints = collect.get(PrimaryConstraint.class.getName());
        if (constraints != null && constraints.size() > 1) {
            throw new SemanticException(SemanticErrorCode.TABLE_PRIMARY_KEY_MUST_ONE, "Multiple primary key defined");
        }
        Set<String> sets = columnDefines.stream().map(x -> x.getColName().getValue().toLowerCase(Locale.ROOT)).collect(
            Collectors.toSet());
        Set<Identifier> constraintNames = new HashSet<>();
        Map<String, SemanticCheck<? extends BaseConstraint>> maps = new HashMap<>(4);
        maps.put(PrimaryConstraint.class.getName(), new PrimaryConstraintSemanticCheck(sets));
        maps.put(DimConstraint.class.getName(), new DimConstraintSemanticCheck(sets));
        maps.put(LevelConstraint.class.getName(), new LevelConstraintSemanticCheck(sets));
        for (BaseConstraint baseConstraint : constraintStatements) {
            if (!constraintNames.add(baseConstraint.getName())) {
                throw new SemanticException(SemanticErrorCode.TABLE_CONSTRAINT_MUST_UNIQUE,
                    "Constraint Name must unique: " + baseConstraint.getName());
            }
            SemanticCheck semanticCheck = maps.get(
                baseConstraint.getClass().getName());
            if (semanticCheck == null) {
                continue;
            }
            semanticCheck.check(baseConstraint);
        }
    }

}
