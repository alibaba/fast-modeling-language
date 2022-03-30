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

package com.aliyun.fastmodel.core.tree.statement.rule.function;

import lombok.Getter;

/**
 * 列函数的名字
 *
 * @author panguanjing
 * @date 2021/5/30
 */
@Getter
public enum BaseFunctionName {
    /**
     * 最大
     */
    MAX(FunctionGrade.COLUMN, "最大值", ""),
    /**
     * 最小
     */
    MIN(FunctionGrade.COLUMN, "最小值", ""),

    /**
     * 平均
     */
    AVG(FunctionGrade.COLUMN, "平均值", ""),
    /**
     * 汇总值
     */
    SUM(FunctionGrade.COLUMN, "汇总值", ""),
    /**
     * 空值个数
     */
    NULL_COUNT(FunctionGrade.COLUMN, "空值数", "非空"),

    /**
     * 空值/总行数
     */
    NULL_RATE(FunctionGrade.COLUMN, "空值率", ""),

    /**
     * 重复值个数
     */
    DUPLICATE_COUNT(FunctionGrade.COLUMN, "重复值数", "唯一"),

    /**
     * 重复值/总行数
     */
    DUPLICATE_RATE(FunctionGrade.COLUMN, "重复率", ""),
    /**
     * 唯一值
     */
    UNIQUE_COUNT(FunctionGrade.COLUMN, "唯一值数", ""),
    /**
     * 离散值分组数
     */
    GROUP_COUNT(FunctionGrade.COLUMN, "离散值分组数", ""),
    /**
     * 离散值状态值
     */
    STATE_COUNT(FunctionGrade.COLUMN, "离散值状态数", ""),

    /**
     * 值是否在表里
     */
    IN_TABLE(FunctionGrade.COLUMN, "表中", ""),

    /**
     * 表行数
     */
    TABLE_COUNT(FunctionGrade.TABLE, "表行数", ""),

    /**
     * 表大小
     */
    TABLE_SIZE(FunctionGrade.TABLE, "表大小", ""),

    /**
     * 表中的唯一键
     */
    UNIQUE(FunctionGrade.TABLE, "表唯一键", "唯一"),

    /**
     * VOL波动率
     */
    VOL(FunctionGrade.VOL, "波动率", "");

    private final FunctionGrade functionGrade;

    private final String description;

    private final String aliasName;

    BaseFunctionName(FunctionGrade functionGrade, String description, String aliasName) {
        this.functionGrade = functionGrade;
        this.description = description;
        this.aliasName = aliasName;
    }

    public boolean isTableFunction() {
        return functionGrade == FunctionGrade.TABLE;
    }

    public boolean isColumnFunction() {
        return functionGrade == FunctionGrade.COLUMN;
    }

    public boolean isVolFunction() {
        return functionGrade == FunctionGrade.VOL;
    }
}
