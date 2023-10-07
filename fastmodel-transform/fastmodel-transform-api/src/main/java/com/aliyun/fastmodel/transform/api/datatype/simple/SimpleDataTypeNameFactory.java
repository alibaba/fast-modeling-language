/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.datatype.simple;

import java.util.List;
import java.util.ServiceLoader;

import com.google.common.collect.Lists;

/**
 * simple dataType name factory
 *
 * @author panguanjing
 * @date 2022/11/7
 */
public class SimpleDataTypeNameFactory  {

    private final List<SimpleDataTypeNameConverter> converterList;

    private SimpleDataTypeNameFactory(){
        ServiceLoader<SimpleDataTypeNameConverter> serviceLoader =
             ServiceLoader.load(SimpleDataTypeNameConverter.class);
        converterList = Lists.newArrayList(serviceLoader);
    }

    private final static SimpleDataTypeNameFactory FACTORY = new SimpleDataTypeNameFactory();

    public static SimpleDataTypeNameFactory getInstance() {
        return FACTORY;
    }

    public SimpleDataTypeName convert(String dataTypeName, SimpleDataTypeName defaultDataTypeName) {
        for (SimpleDataTypeNameConverter simpleDataTypeNameConverter : converterList) {
            try {
                SimpleDataTypeName convert = simpleDataTypeNameConverter.convert(dataTypeName);
                return convert;
            } catch (Exception ignore) {
                //ignore excepton when unknown dataTypeName
                continue;
            }
        }
        return defaultDataTypeName;
    }
}
