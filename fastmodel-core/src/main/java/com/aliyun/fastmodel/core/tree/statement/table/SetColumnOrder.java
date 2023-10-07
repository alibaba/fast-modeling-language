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

package com.aliyun.fastmodel.core.tree.statement.table;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.BaseOperatorStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * set column order
 * https://thispointer.com/mysql-change-column-order/#four
 *
 * @author panguanjing
 * @date 2022/1/25
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class SetColumnOrder extends BaseOperatorStatement {
    /**
     * 原来列的名字
     */
    private final Identifier oldColName;

    /**
     * 新的列名
     */
    private final Identifier newColName;

    /**
     * 数据类型
     */
    private final BaseDataType dataType;

    /**
     * 放在前面的列名
     */
    private final Identifier beforeColName;

    /**
     * 设置是否为第一个
     */
    private final Boolean first;

    public SetColumnOrder(QualifiedName qualifiedName, Identifier oldColName, Identifier newColName, BaseDataType dataType,
                          Identifier beforeColName, Boolean first) {
        super(qualifiedName);
        this.oldColName = oldColName;
        this.newColName = newColName;
        this.dataType = dataType;
        this.beforeColName = beforeColName;
        this.first = first;
        this.setStatementType(StatementType.TABLE);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSetColumnOrder(this, context);
    }
}
