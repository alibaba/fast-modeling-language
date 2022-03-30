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

package com.aliyun.fastmodel.core.tree.statement.constants;

import lombok.Getter;

/**
 * 表格的详细的类型
 *
 * @author panguanjing
 * @date 2020/9/15
 */
public enum TableDetailType {
    /**
     * 普通维度
     */
    NORMAL_DIM(TableType.DIM, "NORMAL", "普通维度表"),

    /**
     * 层级维度
     */
    LEVEL_DIM(TableType.DIM, "LEVEL", "层级维度表"),

    /**
     * 枚举维度
     */
    ENUM_DIM(TableType.DIM, "ENUM", "枚举维度表"),

    /**
     * 事务事实表
     */
    TRANSACTION_FACT(TableType.FACT, "TRANSACTION", "事务事实表"),

    /**
     * Periodic snapshot fact table
     * 周期快照表
     */
    PERIODIC_SNAPSHOT_FACT(TableType.FACT, "PERIODIC_SNAPSHOT", "周期快照表"),

    /**
     * 累积快照表
     * accumulating_snapshot_fact
     */
    ACCUMULATING_SNAPSHOT_FACT(TableType.FACT, "ACCUMULATING_SNAPSHOT", "累积快照表"),

    /**
     * 无事实事实表
     * factless_fact
     */
    FACTLESS_FACT(TableType.FACT, "FACTLESS", "无事实事实表"),

    /**
     * 聚集型事实表
     * aggregate_fact
     */
    AGGREGATE_FACT(TableType.FACT, "AGGREGATE", "聚集性事实表"),

    /**
     * 合并事实表
     */
    CONSOLIDATED_FACT(TableType.FACT, "CONSOLIDATED", "合并事实表"),

    /**
     * 码表
     */
    CODE(TableType.CODE, "CODE", "标准代码表"),

    /**
     * DWS table
     */
    DWS(TableType.DWS, "DWS", "普通汇总表"),

    /**
     * 高级逻辑汇总表
     */
    ADVANCED_DWS(TableType.DWS, "ADVANCED", "轻度汇总表"),

    /**
     * ADS table
     */
    ADS(TableType.ADS, "ADS", "普通应用表")

    ;

    @Getter
    private final TableType parent;

    @Getter
    private final String code;

    @Getter
    private final String comment;

    TableDetailType(TableType parent, String code, String comment) {
        this.parent = parent;
        this.code = code;
        this.comment = comment;
    }

    public boolean isSingle() {
        return getCode().equalsIgnoreCase(getParent().getCode());
    }

    public static TableDetailType getByCode(String code, TableType tableType) {
        TableDetailType[] tableDetailTypes = TableDetailType.values();
        for (TableDetailType t : tableDetailTypes) {
            if (t.getCode().equalsIgnoreCase(code) && t.getParent() == tableType) {
                return t;
            }
        }
        throw new IllegalArgumentException("can't find the table type with code:" + code);
    }

    public static TableDetailType getByCode(String code) {
        TableDetailType[] tableDetailTypes = TableDetailType.values();
        for (TableDetailType t : tableDetailTypes) {
            if (t.getCode().equalsIgnoreCase(code)) {
                return t;
            }
        }
        throw new IllegalArgumentException("can't find the table type with code:" + code);
    }

    public static TableDetailType getByComment(String comment) {
        TableDetailType[] tableDetailTypes = TableDetailType.values();
        for (TableDetailType t : tableDetailTypes) {
            if (t.getComment().equalsIgnoreCase(comment)) {
                return t;
            }
        }
        throw new IllegalArgumentException("can't find the table type with comment:" + comment);
    }
}
