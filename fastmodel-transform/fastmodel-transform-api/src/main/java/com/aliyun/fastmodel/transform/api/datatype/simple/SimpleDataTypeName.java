/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.datatype.simple;

import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;

/**
 * simple data Type name
 *
 * @author panguanjing
 * @date 2022/11/7
 */
public enum SimpleDataTypeName implements IDataTypeName {
    /**
     * number
     */
    NUMBER("NUMBER"),

    /**
     * string
     */
    STRING("STRING"),

    /**
     * date
     */
    DATE("DATE"),

    /**
     * boolean
     */
    BOOLEAN("BOOLEAN");

    private final String value;

    SimpleDataTypeName(String value) {this.value = value;}

    @Override
    public String getName() {
        return this.name();
    }

    @Override
    public String getValue() {
        return this.value;
    }
}
