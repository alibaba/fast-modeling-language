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
 * 列分类
 * <p>
 * 列分类的内容, 目前支持属性，度量，关联维度
 *
 * @author panguanjing
 * @date 2020/12/7
 */
public enum ColumnCategory {

    /**
     * 属性
     */
    ATTRIBUTE("属性"),

    /**
     * 度量
     */
    MEASUREMENT("度量"),

    /**
     * 关联
     */
    CORRELATION("关联维度"),

    /**
     * 分区字段
     */
    PARTITION("分区"),

    /**
     * 指标维度
     */
    REL_DIMENSION("指标维度"),

    /**
     * 关联指标
     */
    REL_INDICATOR("关联指标"),

    /**
     * 统计时间
     */
    STAT_TIME("统计时间");

    @Getter
    private final String comment;

    ColumnCategory(String comment) {
        this.comment = comment;
    }

    public static ColumnCategory getByCode(String code) {
        ColumnCategory[] categories = ColumnCategory.values();
        for (ColumnCategory category : categories) {
            if (category.name().equalsIgnoreCase(code)) {
                return category;
            }
        }
        throw new IllegalArgumentException("can't find the columnCategory with code:" + code);
    }

    public static ColumnCategory getByComment(String comment) {
        ColumnCategory[] categories = ColumnCategory.values();
        for (ColumnCategory category : categories) {
            if (category.getComment().equalsIgnoreCase(comment)) {
                return category;
            }
        }
        throw new IllegalArgumentException("can't find the columnCategory with comment:" + comment);
    }
}
