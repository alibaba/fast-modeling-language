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

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.BaseOperatorStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * 增加列语句
 * <p>
 * DSL举例
 * <pre>
 *         ALTER TABLE unit.table_name1 ADD COLUMNS (c1 bigint comment 'comment')
 *     </pre>
 *
 * @author panguanjing
 * @date 2020/9/7
 */
@Getter
@EqualsAndHashCode(callSuper = true)
public class AddCols extends BaseOperatorStatement {

    /**
     * 增加下
     */
    private final List<ColumnDefinition> columnDefineList;

    public AddCols(QualifiedName qualifiedName, List<ColumnDefinition> columnDefineList) {
        super(qualifiedName);
        this.columnDefineList = columnDefineList;
        setStatementType(StatementType.TABLE);
    }

    @Override
    public List<? extends Node> getChildren() {
        return columnDefineList;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAddCols(this, context);
    }
}
