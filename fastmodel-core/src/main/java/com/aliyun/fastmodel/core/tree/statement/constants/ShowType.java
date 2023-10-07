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
 * showType
 *
 * @author panguanjing
 * @date 2020/12/2
 */
public enum ShowType {
    /**
     * table
     */
    TABLE(StatementType.TABLE),
    /**
     * indicator
     */
    INDICATOR(StatementType.INDICATOR),
    /**
     * adjunct
     */
    ADJUNCT(StatementType.ADJUNCT),
    /**
     * time period
     */
    TIME_PERIOD(StatementType.TIME_PERIOD),
    /**
     * layer
     */
    LAYER(StatementType.LAYER),
    /**
     * measure unit
     */
    MEASURE_UNIT(StatementType.MEASURE_UNIT),

    /**
     * dict
     */
    DICT(StatementType.DICT),

    /**
     * group
     */
    GROUP(StatementType.GROUP),

    /**
     * market
     */
    MARKET(StatementType.MARKET),

    /**
     * subject
     */
    SUBJECT(StatementType.SUBJECT),

    /**
     * business process
     */
    BUSINESS_PROCESS(StatementType.BUSINESSPROCESS),

    /**
     * domain
     */
    DOMAIN(StatementType.DOMAIN),

    /**
     * business_category
     */
    BUSINESS_CATEGORY(StatementType.BUSINESS_CATEGORY);

    private final StatementType statementType;

    private ShowType(StatementType statementType) {
        this.statementType = statementType;
    }

    public String getCode() {
        return statementType.getCode();
    }

    public StatementType getStatementType() {
        return statementType;
    }

    public static ShowType getByCode(String code) {
        StatementType statementType = StatementType.getByCode(code);
        for (ShowType showType : ShowType.values()) {
            if (showType.getStatementType() == statementType) {
                return showType;
            }
        }
        throw new IllegalArgumentException("unsupported show by code:" + code);
    }

}
