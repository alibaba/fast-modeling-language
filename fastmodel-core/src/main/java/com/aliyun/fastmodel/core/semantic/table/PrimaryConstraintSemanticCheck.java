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
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;

/**
 *
 * @author panguanjing
 * @date 2020/10/15
 */
public class PrimaryConstraintSemanticCheck implements SemanticCheck<PrimaryConstraint> {

    private final Set<String> sets;

    public PrimaryConstraintSemanticCheck(Set<String> sets) {this.sets = sets;}

    @Override
    public void check(PrimaryConstraint p) throws SemanticException {
        List<String> colNames = p.getColNames().stream().map(Identifier::getValue).collect(Collectors.toList());
        List<String> notContains = notContains(sets, colNames);
        if (!notContains.isEmpty()) {
            throw new SemanticException(SemanticErrorCode.TABLE_CONSTRAINT_COL_MUST_EXIST,
                "Table constraint can't contains columnName:" + notContains);
        }

    }
}
