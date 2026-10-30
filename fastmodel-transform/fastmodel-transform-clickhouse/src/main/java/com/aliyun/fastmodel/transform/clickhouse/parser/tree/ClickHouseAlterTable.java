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

package com.aliyun.fastmodel.transform.clickhouse.parser.tree;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * ClickHouse 的 ALTER TABLE 语句(分区相关子句)
 * <p>
 * ClickHouse 的分区运维语句(DROP/DETACH/ATTACH/FREEZE/MOVE/REPLACE PARTITION)在 core 里没有对应语句模型，
 * 这里用方言扩展节点承载，支持 reverse(SQL) -&gt; transform(SQL) 的双向保真。
 *
 * @author panguanjing
 */
@Getter
@EqualsAndHashCode(callSuper = true)
public class ClickHouseAlterTable extends BaseStatement {

    /**
     * 表名
     */
    private final QualifiedName tableName;

    /**
     * ON CLUSTER 的集群名原文，可为空
     */
    private final String cluster;

    /**
     * 子句列表，按 SQL 中的顺序保序
     */
    private final List<ClickHouseAlterTableClause> clauses;

    @Builder
    public ClickHouseAlterTable(QualifiedName tableName, String cluster, List<ClickHouseAlterTableClause> clauses) {
        this.tableName = tableName;
        this.cluster = cluster;
        this.clauses = clauses == null ? Lists.newArrayList() : clauses;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }
}
