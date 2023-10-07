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

package com.aliyun.fastmodel.core.tree.statement.script;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.BaseOperatorStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import lombok.Getter;

/**
 * import script statement
 *
 * @author panguanjing
 * @date 2021/9/14
 */
@Getter
public class ImportObject extends BaseOperatorStatement {

    private final Identifier alias;

    public ImportObject(QualifiedName qualifiedName, Identifier alias) {
        super(qualifiedName);
        this.alias = alias;
        setStatementType(StatementType.SCRIPT);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitImportEntityStatement(this, context);
    }
}
