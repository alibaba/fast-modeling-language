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
 * @author panguanjing
 * @date 2020/9/3
 */

public enum StatementType {

    /**
     * 业务板块
     */
    BUSINESSUNIT("business_unit", "unit"),
    /**
     * domain
     */
    DOMAIN("domain", "domain"),
    /**
     * businessprocess
     */
    BUSINESSPROCESS("business_process", "bp"),

    /**
     * 业务分类
     */
    BUSINESS_CATEGORY("business_category", "bc"),
    /**
     * table
     */
    TABLE("table", "table"),
    /**
     * indicator
     */
    INDICATOR("indicator", "indicator"),

    /**
     * 物化类型
     */
    MATERIALIZE("materialize", "materialize"),

    /**
     * 数据字典
     */
    DICT("dict", "Dict"),

    /**
     * 度量单位
     */
    MEASURE_UNIT("measure_unit", "mu"),

    /**
     * 分组
     */
    GROUP("group", "group"),
    /**
     * 数仓分层
     */
    LAYER("layer", "layer"),

    /**
     * 修饰词
     */
    ADJUNCT("adjunct", "adjunct"),
    /**
     * 描述信息
     */
    DESCRIBE("describe", "describe"),

    /**
     * 时间周期
     */
    TIME_PERIOD("time_period", "TimePeriod"),

    /**
     * Show Statements
     */
    SHOW("show", "Show"),

    /**
     * 显示创建的语句
     */
    SHOW_CREATE("showCreate", "showCreate"),

    /**
     * 查询语句
     */
    SELECT("select", "select"),

    /**
     * 删除语句类型
     */
    DELETE("delete", "Delete"),

    /**
     * 删除语句
     */
    INSERT("insert", "Insert"),

    /**
     * USE 语句
     */
    USE("use", "Use"),

    /**
     * 批量操作
     */
    BATCH("batch", "Batch"),

    /**
     * Call
     */
    CALL("call", "Call"),

    /**
     * Pipe
     */
    PIPE("pipe", "Pipe"),

    /**
     * composite
     */
    COMPOSITE("composite", "Composite"),

    /**
     * Export
     */
    EXPORT("export", "Export"),
    /**
     * script
     */
    SCRIPT("script", "Script"),

    /**
     * dimensionality
     */
    DIMENSION("dimension", "Dimension"),

    /**
     * subject
     */
    SUBJECT("subject", "Subject"),

    /**
     * market
     */
    MARKET("market", "Market"),

    /**
     * references
     */
    REFERENCES("references", "References"),

    /**
     * 命令
     */
    COMMAND("command", "Command");

    private final String code;

    private final String shortCode;

    StatementType(String code, String shortCode) {
        this.shortCode = shortCode;
        this.code = code;
    }

    public String getShortCode() {
        return shortCode;
    }

    public String getCode() {
        return code;
    }

    public static StatementType getByCode(String code) {
        StatementType[] statementTypes = StatementType.values();
        for (StatementType statementType : statementTypes) {
            if (statementType.getCode().equalsIgnoreCase(code)) {
                return statementType;
            }
            if (statementType.getShortCode().equalsIgnoreCase(code)) {
                return statementType;
            }
        }
        throw new IllegalArgumentException("code can't find createType,with:" + code);
    }
}
