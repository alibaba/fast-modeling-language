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

package com.aliyun.fastmodel.core.tree.statement.impexp;

import java.util.List;

import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.BaseQueryStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * export statement
 * export output=a.txt where domain='cn';
 *
 * @author panguanjing
 * @date 2021/8/20
 */
@Getter
public class ExportStatement extends BaseQueryStatement {

    private final StringLiteral output;

    private final BaseExpression where;

    private final StringLiteral target;

    public ExportStatement(Identifier baseUnit, StringLiteral output,
                           StringLiteral target, BaseExpression where) {
        super(baseUnit);
        this.output = output;
        this.target = target;
        this.where = where;
        setStatementType(StatementType.EXPORT);
    }

    public String getOutputValue() {
        if (output != null) {
            return output.getValue();
        }
        return StringUtils.EMPTY;
    }

    public String getTargetValue() {
        if (target != null) {
            return target.getValue();
        }
        return StringUtils.EMPTY;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(output, where);
    }
}
