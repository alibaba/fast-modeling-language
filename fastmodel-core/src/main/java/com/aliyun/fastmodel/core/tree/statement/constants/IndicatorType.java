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

/**
 * 指标类型
 *
 * @author panguanjing
 * @date 2020/9/14
 */
public enum IndicatorType {
    /**
     * 原子指标
     */
    ATOMIC("atomic", "Atomic Indicator"),

    /**
     * 复合指标
     */
    ATOMIC_COMPOSITE("atomic_composite", "Atomic Composite Indicator"),

    /**
     * 衍生指标
     */
    DERIVATIVE("derivative", "Derivative Indicator"),

    /**
     * 复合衍生指标
     */
    DERIVATIVE_COMPOSITE("derivative_composite", " Derivative Composite Indicator");

    /**
     * 指标的code
     */
    private final String code;

    /**
     * 描述
     */
    private final String description;

    IndicatorType(String code, String description) {
        this.code = code;
        this.description = description;
    }

    public String getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

    public static IndicatorType getByCode(String code) {
        IndicatorType[] indicatorTypes = IndicatorType.values();
        for (IndicatorType indicatorType : indicatorTypes) {
            if (indicatorType.getCode().equalsIgnoreCase(code)) {
                return indicatorType;
            }
        }
        throw new IllegalArgumentException("can't find the code mapping type, code:" + code);
    }
}
