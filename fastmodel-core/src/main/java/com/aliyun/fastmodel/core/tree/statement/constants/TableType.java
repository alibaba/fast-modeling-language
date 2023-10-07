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

import com.aliyun.fastmodel.core.tree.statement.table.type.ITableType;
import lombok.Getter;

/**
 * 表类型
 *
 * @author panguanjing
 * @date 2020/9/4
 */
@Getter
public enum TableType implements ITableType {

    /**
     * 维度表
     */
    DIM("dim", "Dim Table"),

    /**
     * 事实表
     */
    FACT("fact", "Fact Table"),

    /**
     * 码表
     */
    CODE("code", "Code Table"),

    /**
     * 数据汇总表，用于指标中的dws表定义
     */
    DWS("dws", "Dws Table"),

    /**
     * 数据汇总表，用于指标中的dws表定义
     */
    ADS("ads", "ADS Table"),

    /**
     * 贴源表
     */
    ODS("ods", "ODS Table"),
    ;
    /**
     * 编码
     */
    private final String code;
    /**
     * 描述
     */
    private final String description;

    TableType(String code, String description) {
        this.code = code;
        this.description = description;
    }

    public static TableType getByCode(String code) {
        TableType[] tableTypes = TableType.values();
        for (TableType t : tableTypes) {
            if (t.getCode().equalsIgnoreCase(code)) {
                return t;
            }
        }
        throw new IllegalArgumentException("can't find the tableType with code:" + code);
    }

}
