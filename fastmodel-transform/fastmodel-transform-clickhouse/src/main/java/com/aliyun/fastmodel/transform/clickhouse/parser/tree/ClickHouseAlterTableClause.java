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

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * ClickHouse ALTER TABLE 的分区子句
 *
 * @author panguanjing
 */
@Getter
@EqualsAndHashCode
public class ClickHouseAlterTableClause {

    /**
     * 子句类型
     */
    public enum ClauseType {

        /**
         * DROP PARTITION
         */
        DROP_PARTITION,

        /**
         * DETACH PARTITION
         */
        DETACH_PARTITION,

        /**
         * ATTACH PARTITION
         */
        ATTACH_PARTITION,

        /**
         * FREEZE [PARTITION]
         */
        FREEZE,

        /**
         * MOVE PARTITION ... TO DISK
         */
        MOVE_PARTITION_TO_DISK,

        /**
         * MOVE PARTITION ... TO VOLUME
         */
        MOVE_PARTITION_TO_VOLUME,

        /**
         * MOVE PARTITION ... TO TABLE
         */
        MOVE_PARTITION_TO_TABLE,

        /**
         * REPLACE PARTITION ... FROM
         */
        REPLACE_PARTITION
    }

    private final ClauseType type;

    /**
     * 分区表达式原文，如 '202401'、ID '202401'、toYYYYMM(ts)
     */
    private final String partition;

    /**
     * ATTACH/REPLACE 的 FROM 表名原文
     */
    private final String fromTable;

    /**
     * MOVE 的目标(磁盘/卷/表)原文
     */
    private final String target;

    @Builder
    public ClickHouseAlterTableClause(ClauseType type, String partition, String fromTable, String target) {
        this.type = type;
        this.partition = partition;
        this.fromTable = fromTable;
        this.target = target;
    }
}
