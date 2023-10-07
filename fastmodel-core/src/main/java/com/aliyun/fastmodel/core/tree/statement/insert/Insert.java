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

package com.aliyun.fastmodel.core.tree.statement.insert;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.Row;
import com.aliyun.fastmodel.core.tree.statement.BaseOperatorStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.aliyun.fastmodel.core.tree.statement.rule.PartitionSpec;
import com.aliyun.fastmodel.core.tree.statement.select.Query;
import com.aliyun.fastmodel.core.tree.util.QueryUtil;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * 插入语句内容
 *
 * @author panguanjing
 * @date 2020/9/16
 */
@Getter
public class Insert extends BaseOperatorStatement {

    /**
     * 是否全量覆盖
     */
    private final Boolean overwrite;

    /**
     * 分区的信息x
     */
    private final List<PartitionSpec> partitionSpecList;
    /**
     * 查询内容
     */
    private final Query query;
    /**
     * 写入的列
     */
    private final List<Identifier> columns;

    public Insert(Boolean overwrite, QualifiedName target,
                  List<PartitionSpec> partitionSpecList,
                  Query query,
                  List<Identifier> columns) {
        super(target);
        this.overwrite = overwrite;
        this.partitionSpecList = partitionSpecList;
        this.query = query;
        this.columns = columns;
        setStatementType(StatementType.INSERT);
    }

    public Insert(QualifiedName target,
                  Query query,
                  List<Identifier> columns) {
        this(false, target, null, query, columns);
    }

    public Insert(Boolean overwrite,
                  QualifiedName target,
                  Row[] rows,
                  List<Identifier> columns) {
        this(overwrite, target, null,
            QueryUtil.query(QueryUtil.values(rows)), columns);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(query);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitInsert(this, context);
    }
}

