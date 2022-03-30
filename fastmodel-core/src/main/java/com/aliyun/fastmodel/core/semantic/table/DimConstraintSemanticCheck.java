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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.exception.SemanticException;
import com.aliyun.fastmodel.core.semantic.SemanticCheck;
import com.aliyun.fastmodel.core.semantic.SemanticErrorCode;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.DimConstraint;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/10/15
 */
public class DimConstraintSemanticCheck implements SemanticCheck<DimConstraint> {

    private final Set<String> columns;

    public DimConstraintSemanticCheck(Set<String> columns) {
        this.columns = columns;
    }

    @Override
    public void check(DimConstraint dimConstraintStatement) throws SemanticException {
        if (dimConstraintStatement.getColNames() == null) {
            //如果为空，那么就做校验了
            return;
        }
        List<String> colNames = dimConstraintStatement.getColNames().stream()
            .map(Identifier::getValue).collect(Collectors.toList());
        if (!isEmpty(colNames)) {
            List<String> notContains = notContains(columns, colNames);
            if (!isEmpty(notContains)) {
                throw new SemanticException(SemanticErrorCode.TABLE_CONSTRAINT_COL_MUST_EXIST,
                    "Table constraint haven't contains columnName:" + notContains);
            }
            List<String> referenceColNames =
                dimConstraintStatement.getReferenceColNames().stream().map(Identifier::getValue).collect(
                    Collectors.toList());
            if (isEmpty(referenceColNames)) {
                throw new SemanticException(
                    SemanticErrorCode.TABLE_CONSTRAINT_REF_SIZE_MUST_EQUAL,
                    "Table constraint dim reference column size not equal, expect:" + colNames.size()
                        + ", but:0");
            }
            if (colNames.size() != referenceColNames.size()) {
                throw new SemanticException(
                    SemanticErrorCode.TABLE_CONSTRAINT_REF_SIZE_MUST_EQUAL,
                    "Table constraint dim reference column size not equal, expect:" + colNames.size() + ", but:"
                        + referenceColNames.size());
            }
        }
    }
}
