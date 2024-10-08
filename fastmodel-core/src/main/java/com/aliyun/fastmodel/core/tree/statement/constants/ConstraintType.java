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
@Getter
public enum ConstraintType {
    /**
     * 主键约束
     */
    PRIMARY_KEY(Constants.PRIMARY, ConstraintScope.COLUMN, "Primary Key Constraint"),
    /**
     * 维度约束
     */
    DIM_KEY(Constants.DIM, ConstraintScope.COLUMN, "Dim Constraint"),

    /**
     * 层级约束
     */
    LEVEL_KEY(Constants.LEVEL, ConstraintScope.COLUMN, "Level Constraint"),

    /**
     * 不为空
     */
    NOT_NULL(Constants.NOT_NULL, ConstraintScope.COLUMN, "notNull constraint"),

    /**
     * 列组的约束
     */
    COLUMN_GROUP(Constants.COLUMN_GROUP, ConstraintScope.COLUMN, "column Group constraint"),

    /**
     * 时间周期类型
     */
    TIME_PERIOD(Constants.TIME_PERIOD, ConstraintScope.TABLE, "time period"),

    /**
     * UNIQUE
     */
    UNIQUE(Constants.UNIQUE, ConstraintScope.COLUMN, "唯一值约束"),

    /**
     * Redundant冗余约束
     */
    REDUNDANT(Constants.REDUNDANT, ConstraintScope.COLUMN, "冗余约束"),

    /**
     * 默认值约束
     */
    DEFAULT_VALUE(Constants.DEFAULT_VALUE, ConstraintScope.COLUMN, "默认值"),

    /**
     * 自定义约束信息
     */
    CHECK(Constants.CHECK, ConstraintScope.UNDEFINED, "自定义约束"),
    /**
     * index
     */
    INDEX(Constants.INDEX, ConstraintScope.UNDEFINED, "索引"),

    ;

    /**
     * 唯一
     */
    private final String code;
    /**
     * 作用域
     */
    private final ConstraintScope scope;
    /**
     * 约束描述
     */
    private final String description;

    public static class Constants {
        public static final String PRIMARY = "primary";
        public static final String DIM = "dim";
        public static final String LEVEL = "level";
        public static final String NOT_NULL = "notNull";
        public static final String COLUMN_GROUP = "column_group";
        public static final String TIME_PERIOD = "time_period";
        public static final String UNIQUE = "unique";
        public static final String REDUNDANT = "redundant";
        public static final String DEFAULT_VALUE = "default_value";
        public static final String CHECK = "check";
        public static final String INDEX = "index";
    }

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
