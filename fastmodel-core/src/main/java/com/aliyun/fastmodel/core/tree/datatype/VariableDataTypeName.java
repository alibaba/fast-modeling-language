/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.core.tree.datatype;

import lombok.Data;

/**
 * 经常变化的名字
 *
 * @author panguanjing
 * @date 2022/6/9
 */
@Data
public class VariableDataTypeName implements IDataTypeName {
    /**
     * name
     */
    private final String name;

    /**
     * value
     */
    private final String value;

    public VariableDataTypeName(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public VariableDataTypeName(String value) {
        this(value, value);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getValue() {
        return value;
    }
}
