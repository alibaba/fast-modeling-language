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
 * 约束类型
 *
 * @author panguanjing
 * @date 2020/9/4
 */
public enum ConstraintType {
    /**
     * 主键约束
     */
    PRIMARY_KEY("primary", ConstraintScope.COLUMN, "Primary Key Constraint"),
    /**
     * 维度约束
     */
    DIM_KEY("dim", ConstraintScope.COLUMN, "Dim Constraint"),

    /**
     * 层级约束
     */
    LEVEL_KEY("level", ConstraintScope.COLUMN, "Level Constraint"),

    /**
     * 不为空
     */
    NOT_NULL("notNull", ConstraintScope.COLUMN, "notNull constraint"),

    /**
     * 列组的约束
     */
    COLUMN_GROUP("column_group", ConstraintScope.COLUMN, "column Group constraint"),

    /**
     * 时间周期类型
     */
    TIME_PERIOD("time_period", ConstraintScope.TABLE, "time period"),

    /**
     * UNIQUE
     */
    UNIQUE("unique", ConstraintScope.COLUMN, "唯一值约束"),

    /**
     * Redundant冗余约束
     */
    REDUNDANT("redundant", ConstraintScope.COLUMN, "冗余约束"),

    /**
     * 默认值约束
     */
    DEFAULT_VALUE("default_value", ConstraintScope.COLUMN, "默认值"),

    /**
     * 自定义约束信息
     */
    CUSTOM_CONSTRAINT("custom_constraint", ConstraintScope.UNDEFIND, "自定义约束"),

    ;

    /**
     * 唯一
     */
    @Getter
    private final String code;
    /**
     * 作用域
     */
    @Getter
    private final ConstraintScope scope;
    /**
     * 约束描述
     */
    @Getter
    private final String description;

    ConstraintType(String code, ConstraintScope scope, String description) {
        this.code = code;
        this.scope = scope;
        this.description = description;
    }

    public static ConstraintType getByCode(String code) {
        ConstraintType[] constraintTypes = ConstraintType.values();
        for (ConstraintType constraintType : constraintTypes) {
            if (constraintType.getCode().equalsIgnoreCase(code)) {
                return constraintType;
            }
        }
        throw new IllegalArgumentException("code can't find constraintType,with:" + code);
    }

}
