/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.client.converter;

import java.util.Map;
import java.util.function.Function;

import com.aliyun.fastmodel.transform.api.client.PropertyConverter;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.property.StringProperty;
import com.google.common.base.Preconditions;

/**
 * BasePropertyConverter
 *
 * @author panguanjing
 * @date 2022/8/5
 */
public abstract class BasePropertyConverter implements PropertyConverter {

    public BasePropertyConverter() {
    }

    @Override
    public BaseClientProperty create(String name, String value) {
        Preconditions.checkNotNull(name, "name can't be null");
        Function<String, BaseClientProperty> baseClientPropertyFunction = getFunctionMap().get(name.toLowerCase());
        if (baseClientPropertyFunction == null) {
            if (!returnDefaultWhenNotExist()) {
                return null;
            }
            StringProperty stringProperty = new StringProperty();
            stringProperty.setKey(name);
            stringProperty.setValue(value);
            return stringProperty;
        }
        return baseClientPropertyFunction.apply(value);
    }

    /**
     * init function map
     *
     * @return Map
     */
    protected abstract Map<String, Function<String, BaseClientProperty>> getFunctionMap();

    /**
     * if key not define then return default
     *
     * @return
     */
    protected boolean returnDefaultWhenNotExist() {
        return true;
    }

}
