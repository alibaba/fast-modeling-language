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
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.BaseOperatorStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.aliyun.fastmodel.core.tree.statement.table.index.IndexSortKey;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * 添加索引
 *
 * @author panguanjing
 * @date 2021/8/31
 */
@Getter
@EqualsAndHashCode
public class CreateIndex extends BaseOperatorStatement {

    private final QualifiedName tableName;

    private final List<IndexSortKey> indexColumnNameList;

    private final List<Property> propertyList;

    public CreateIndex(QualifiedName qualifiedName, QualifiedName tableName,
        List<IndexSortKey> indexColumnNameList,
        List<Property> propertyList) {
        super(qualifiedName);
        this.tableName = tableName;
        this.indexColumnNameList = indexColumnNameList;
        this.propertyList = propertyList;
        setStatementType(StatementType.TABLE);
    }

    public CreateIndex(NodeLocation nodeLocation, String origin,
        QualifiedName qualifiedName, QualifiedName tableName,
        List<IndexSortKey> indexColumnNameList,
        List<Property> propertyList) {
        super(nodeLocation, origin, qualifiedName);
        this.tableName = tableName;
        this.indexColumnNameList = indexColumnNameList;
        this.propertyList = propertyList;
        setStatementType(StatementType.TABLE);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateIndex(this, context);
    }
}
