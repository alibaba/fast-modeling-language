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

package com.aliyun.fastmodel.core.tree.expr.enums;

/**
 * @author panguanjing
 * @date 2020/10/3
 */
public enum IntervalQualifiers {

    /**
     * year
     */
    YEAR("YEAR"),

    /**
     * month
     */
    MONTH("MONTH"),

    /**
     * day
     */
    DAY("DAY"),

    /**
     * year to month
     */
    WEEK("WEEK"),

    /**
     * 季度
     */
    QUARTER("QUARTER"),
    /**
     * hour
     */
    HOUR("HOUR"),

    /**
     *
     */
    MINUTE("MINUTE"),

    /**
     *
     */
    SECOND("SECOND");

    private final String code;

    private IntervalQualifiers(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public static IntervalQualifiers getIntervalQualifiers(String code) {
        IntervalQualifiers[] intervalQualifiers = IntervalQualifiers.values();
        for (IntervalQualifiers intervalQualifiers1 : intervalQualifiers) {
            if (intervalQualifiers1.getCode().equalsIgnoreCase(code)) {
                return intervalQualifiers1;
            }
        }
        throw new IllegalArgumentException("can't find the intervalQualifiers code:" + code);
    }

}
