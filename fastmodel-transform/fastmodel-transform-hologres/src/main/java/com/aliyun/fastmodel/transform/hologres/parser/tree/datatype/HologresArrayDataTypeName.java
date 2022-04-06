/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.parser.tree.datatype;

import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import lombok.Data;

/**
 * array data type name
 *
 * @author panguanjing
 * @date 2022/6/9
 */
@Data
public class HologresArrayDataTypeName implements IDataTypeName {

    public static final String VALUE_SUFFIX = "[]";

    public static final String SUFFIX = "_ARRAY";

    private final IDataTypeName source;

    public HologresArrayDataTypeName(IDataTypeName source) {
        this.source = source;
    }

    /**
     * 根据传入的值判断是否为数组类型
     *
     * @param value
     * @return {@link HologresDataTypeName}
     */
    public static HologresArrayDataTypeName getByValue(String value) {
        int index = value.indexOf(VALUE_SUFFIX);
        if (index < 0) {
            throw new IllegalArgumentException("can't find the dataType with value:" + value);
        }
        String source = value.substring(0, index);
        return new HologresArrayDataTypeName(HologresDataTypeName.getByValue(source));
    }

    @Override
    public String getName() {
        return source.getName() + SUFFIX;
    }

    @Override
    public String getValue() {
        return source.getValue() + VALUE_SUFFIX;
    }

}
