/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.clickhouse.parser.tree;

import com.aliyun.fastmodel.core.tree.statement.table.type.ITableDetailType;
import com.aliyun.fastmodel.core.tree.statement.table.type.ITableType;

/**
 * table type
 *
 * @author panguanjing
 * @date 2022/7/10
 */
public enum ClickHouseTableType implements ITableDetailType {
    /**
     * normal
     */
    NORMAL("normal", "Normal"),

    /**
     * temporary
     */
    TEMPORARY("temporary", "Temp");

    private final String code;

    private final String description;

    ClickHouseTableType(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public ITableType getParent() {
        return this;
    }
}
