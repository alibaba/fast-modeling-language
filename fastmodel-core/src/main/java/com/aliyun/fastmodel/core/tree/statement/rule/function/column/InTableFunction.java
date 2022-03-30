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

package com.aliyun.fastmodel.core.tree.statement.rule.function.column;

import java.util.List;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunctionName;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * 是否在表里内容
 *
 * @author panguanjing
 * @date 2021/5/31
 */
@Getter
public class InTableFunction extends BaseFunction {

    /**
     * 具体在什么表里
     */
    private final QualifiedName tableName;

    /**
     * 匹配的字段名
     */
    private final QualifiedName matchColumn;

    /**
     * 在表里的字段
     */
    private final Identifier inTableColumn;

    public InTableFunction(QualifiedName matchColumn, QualifiedName tableName,
                           Identifier inTableColumn) {
        this.tableName = tableName;
        this.matchColumn = matchColumn;
        this.inTableColumn = inTableColumn == null ? new Identifier("code") : inTableColumn;
    }

    @Override
    public BaseFunctionName funcName() {
        return BaseFunctionName.IN_TABLE;
    }

    @Override
    public List<BaseExpression> arguments() {
        TableOrColumn inTable = new TableOrColumn(tableName);
        TableOrColumn value = new TableOrColumn(matchColumn);
        TableOrColumn e3 = new TableOrColumn(QualifiedName.of(inTableColumn.getValue()));
        return ImmutableList.of(value, inTable, e3);
    }

}
