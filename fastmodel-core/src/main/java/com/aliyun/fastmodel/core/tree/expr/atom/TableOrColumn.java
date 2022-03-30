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

package com.aliyun.fastmodel.core.tree.expr.atom;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.enums.VarType;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * 用于表达表的列的信息，比如： table.field
 *
 * @author panguanjing
 * @date 2020/9/23
 */
@Getter
@EqualsAndHashCode(callSuper = false)
@Setter
public class TableOrColumn extends BaseExpression {

    private QualifiedName qualifiedName;

    private VarType varType;

    public TableOrColumn(NodeLocation location, String origin,
                         QualifiedName qualifiedName, VarType varType) {
        super(location, origin);
        this.qualifiedName = qualifiedName;
        this.varType = varType;
    }

    public TableOrColumn(QualifiedName qualifiedName, VarType varType) {
        this(null, null, qualifiedName, varType);
    }

    public TableOrColumn(QualifiedName qualifiedName) {
        this(qualifiedName, null);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitTableOrColumn(this, context);
    }
}
