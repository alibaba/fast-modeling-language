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
 * comparison operator
 *
 * @author panguanjing
 * @date 2020/11/4
 */
public enum ComparisonOperator {

    /**
     * 小于等于
     */
    LESS_THAN_OR_EQUAL("<="),
    /**
     * 小于
     */
    LESS_THAN("<"),
    /**
     * 大于等于
     */
    GREATER_THAN_OR_EQUAL(">="),
    /**
     * 大于
     */
    GREATER_THAN(">"),

    /**
     * equal
     */
    EQUAL("="),

    /**
     * not equal
     */
    NOT_EQUAL("<>"),

    /**
     * not equal
     */
    NOT_EQUAL_MS("!="),

    NS_EQUAL("<=>"),

    /**
     * is distinct from
     */
    IS_DISTINCT_FROM("IS DISTINCT FROM");

    /**
     * code
     */
    private final String code;

    private ComparisonOperator(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public static ComparisonOperator getByCode(String text) {
        ComparisonOperator[] comparisonOperators = ComparisonOperator.values();
        for (ComparisonOperator operator : comparisonOperators) {
            if (operator.getCode().equalsIgnoreCase(text)) {
                return operator;
            }
        }
        throw new IllegalArgumentException("can't find the operator with: " + text);
    }
}
