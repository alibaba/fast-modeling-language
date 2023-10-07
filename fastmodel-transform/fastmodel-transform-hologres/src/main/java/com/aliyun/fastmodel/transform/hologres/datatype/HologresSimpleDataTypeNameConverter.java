/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.datatype;

import com.aliyun.fastmodel.transform.api.datatype.simple.ISimpleDataTypeName;
import com.aliyun.fastmodel.transform.api.datatype.simple.SimpleDataTypeName;
import com.aliyun.fastmodel.transform.api.datatype.simple.SimpleDataTypeNameConverter;
import com.aliyun.fastmodel.transform.hologres.parser.tree.datatype.HologresDataTypeName;
import com.google.auto.service.AutoService;

/**
 * simple Data Type Name converter
 *
 * @author panguanjing
 * @date 2022/11/7
 */
@AutoService(SimpleDataTypeNameConverter.class)
public class HologresSimpleDataTypeNameConverter implements SimpleDataTypeNameConverter{
    @Override
    public SimpleDataTypeName convert(String dataTypeName) {
        ISimpleDataTypeName byValue = HologresDataTypeName.getByValue(dataTypeName);
        return byValue.getSimpleDataTypeName();
    }
}
