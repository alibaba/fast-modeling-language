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
public enum ShowObjectsType {
    /**
     * table
     */
    TABLES("TABLES"),

    /**
     * Dim Table
     */
    DIM_TABLES("DIM TABLES"),

    /**
     * Fact table
     */
    FACT_TABLES("FACT TABLES"),

    /**
     * Dws table
     */
    DWS_TABLES("DWS TABLES"),

    /**
     * ADS table
     */
    ADS_TABLES("ADS TABLES"),

    /**
     * code Table
     */
    CODE_TABLES("CODE TABLES"),

    /**
     * indicators
     */
    INDICATORS("INDICATORS"),

    /**
     * ATOMIC indicator
     */
    ATOMIC_INDICATORS("ATOMIC INDICATORS"),

    /**
     * Atomic composite
     */
    ATOMIC_COMPOSITE_INDICATORS("ATOMIC COMPOSITE INDICATORS"),

    /**
     * Derivative indicators
     */
    DERIVATIVE_INDICATORS("DERIVATIVE INDICATORS"),

    /**
     * Derivative Composite indicators
     */
    DERIVATIVE_COMPOSITE_INDICATORS("DERIVATIVE COMPOSITE INDICATORS"),

    /**
     * adjunct
     */
    ADJUNCTS("ADJUNCTS"),
    /**
     * time period
     */
    TIME_PERIODS("TIME_PERIODS"),

    /**
     * layer
     */
    LAYERS("LAYERS"),
    /**
     * measure unit
     */
    MEASURE_UNITS("MEASURE_UNITS"),

    /**
     * dict
     */
    DICTS("DICTS"),

    /**
     * business Processes
     */
    BUSINESS_PROCESSES("BUSINESS_PROCESSES"),

    /**
     * ?????????
     */
    DOMAINS("DOMAINS"),

    /**
     * ?????????
     */
    DICT_GROUPS("DICT GROUPS"),

    /**
     * ???????????????
     */
    MEASURE_UNIT_GROUPS("MEASURE_UNIT GROUPS"),

    /**
     * ?????????????????????
     */
    COLUMNS("COLUMNS"),

    /**
     * ???????????????????????????
     */
    CODES("CODES"),

    /**
     * ??????????????????
     */
    BUSINESS_CATEGORIES("BUSINESS_CATEGORIES"),

    /**
     * ????????????
     */
    MARKETS("MARKETS"),

    /**
     * ?????????
     */
    SUBJECTS("SUBJECTS"),

    /**
     * ??????
     */
    DIMENSIONS("DIMENSIONS"),

    /**
     * ????????????
     */
    DIM_ATTRIBUTES("DIM_ATTRIBUTES");

    /**
     * ?????????code??????
     */
    private final String code;

    ShowObjectsType(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public static ShowObjectsType getByCode(String code) {
        for (ShowObjectsType showType : ShowObjectsType.values()) {
            String s = showType.getCode().replaceAll(" ", "");
            if (s.equalsIgnoreCase(code)) {
                return showType;
            }
        }
        throw new IllegalArgumentException("unsupported show by code:" + code);
    }

}
