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
     * naming dict
     */
    NAMING_DICTS("NAMING DICTS"),

    /**
     * business Processes
     */
    BUSINESS_PROCESSES("BUSINESS_PROCESSES"),

    /**
     * 数据域
     */
    DOMAINS("DOMAINS"),

    /**
     * 标准集
     */
    DICT_GROUPS("DICT GROUPS"),

    /**
     * 度量单位组
     */
    MEASURE_UNIT_GROUPS("MEASURE_UNIT GROUPS"),

    /**
     * 返回表的列信息
     */
    COLUMNS("COLUMNS"),

    /**
     * 返回标准代码的内容
     */
    CODES("CODES"),

    /**
     * 显示业务分类
     */
    BUSINESS_CATEGORIES("BUSINESS_CATEGORIES"),

    /**
     * 数据集市
     */
    MARKETS("MARKETS"),

    /**
     * 主题域
     */
    SUBJECTS("SUBJECTS"),

    /**
     * 维度
     */
    DIMENSIONS("DIMENSIONS"),

    /**
     * 维度属性
     */
    DIM_ATTRIBUTES("DIM_ATTRIBUTES"),

    /**
     * 物化信息
     */
    MATERIALIZED_VIEWS("MATERIALIZED VIEWS");

    /**
     * 对应的code信息
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
