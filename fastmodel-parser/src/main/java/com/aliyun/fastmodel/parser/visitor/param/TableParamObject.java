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

package com.aliyun.fastmodel.parser.visitor.param;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.fastmodel.core.exception.SemanticException;
import com.aliyun.fastmodel.core.semantic.SemanticErrorCode;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.index.TableIndex;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.parser.AstBuilder;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ColumnDefinitionContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ColumnNameTypeOrConstraintContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ColumnNameTypeOrConstraintListContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CreateTableStatementContext;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.BooleanUtils;

/**
 * 参数对象，用于代码复用
 *
 * @author panguanjing
 */
public class TableParamObject {
    private final List<ColumnDefinition> partitionColumnDefines;
    private final List<ColumnDefinition> columnDefines;
    private final List<BaseConstraint> constraints;
    private final List<TableIndex> tableIndexList;

    public TableParamObject(List<ColumnDefinition> partitionColumnDefines,
                            List<ColumnDefinition> columnDefines,
                            List<BaseConstraint> constraints,
                            List<TableIndex> tableIndexList) {
        this.partitionColumnDefines = partitionColumnDefines;
        this.columnDefines = columnDefines;
        this.constraints = constraints;
        this.tableIndexList = tableIndexList;
    }

    public void builderColumns(CreateTableStatementContext ctx, AstBuilder astBuilder) {
        Map<Identifier, ColumnDefinition> map = new HashMap<>(getColumnDefines().size());
        for (ColumnDefinition c : getPartitionColumnDefines()) {
            map.put(c.getColName(), c);
        }
        ColumnNameTypeOrConstraintListContext columnNameTypeOrConstraintListContext = ctx
            .columnNameTypeOrConstraintList();
        if (columnNameTypeOrConstraintListContext == null) {
            return;
        }
        List<ColumnNameTypeOrConstraintContext> columnNameTypeOrConstraintContexts
            = columnNameTypeOrConstraintListContext.columnNameTypeOrConstraint();
        for (ColumnNameTypeOrConstraintContext context : columnNameTypeOrConstraintContexts) {
            ColumnDefinitionContext columnDefinitionContext = context.columnDefinition();
            if (columnDefinitionContext != null) {
                ColumnDefinition define = (ColumnDefinition)astBuilder.visit(columnDefinitionContext);
                getColumnDefines().add(define);
                map.put(define.getColName(), define);
                //如果是主键的约束，也加到tableConstraint的list里面
                if (BooleanUtils.isTrue(define.getPrimary())) {
                    getConstraints().add(
                        new PrimaryConstraint(IdentifierUtil.sysIdentifier(), ImmutableList.of(define.getColName())));
                }
            }
            if (context.tableConstraint() != null) {
                BaseConstraint baseConstraint = (BaseConstraint)astBuilder.visit(context.tableConstraint());
                getConstraints().add(baseConstraint);
            }
            if (context.tableIndex() != null) {
                TableIndex tableIndex = (TableIndex)astBuilder.visit(context.tableIndex());
                tableIndexList.add(tableIndex);
            }
        }
        for (BaseConstraint baseConstraint : getConstraints()) {
            if (!(baseConstraint instanceof PrimaryConstraint)) {
                continue;
            }
            PrimaryConstraint primaryConstraint = (PrimaryConstraint)baseConstraint;
            List<Identifier> colNames = primaryConstraint.getColNames();
            for (Identifier identifier : colNames) {
                ColumnDefinition columnDefinition = map.get(identifier);
                if (columnDefinition == null) {
                    throw new SemanticException(SemanticErrorCode.TABLE_CONSTRAINT_COL_MUST_EXIST,
                        String.format("column [%s] not exists", identifier.getValue()));
                }
                if (BooleanUtils.isNotTrue(columnDefinition.getNotNull())) {
                    columnDefinition.setNotNull(true);
                }
            }
        }
    }

    public List<ColumnDefinition> getPartitionColumnDefines() {
        return partitionColumnDefines;
    }

    public List<ColumnDefinition> getColumnDefines() {
        return columnDefines;
    }

    public List<BaseConstraint> getConstraints() {
        return constraints;
    }
}
