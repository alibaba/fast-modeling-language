/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.mc.client.converter;

import java.util.Map;
import java.util.function.Function;

import com.aliyun.fastmodel.transform.api.client.converter.BasePropertyConverter;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.mc.client.property.LifeCycle;
import com.google.common.collect.Maps;

/**
 * converter
 *
 * @author panguanjing
 * @date 2022/8/5
 */
public class MaxComputePropertyConverter extends BasePropertyConverter {

    private final Map<String, Function<String, BaseClientProperty>> map = Maps.newHashMap();

    public MaxComputePropertyConverter() {
        initFunctionMap();
    }

    private void initFunctionMap() {
        this.map.put(LifeCycle.LIFECYCLE, value -> {
            LifeCycle lifeCycle = new LifeCycle();
            lifeCycle.setValueString(value);
            return lifeCycle;
        });
    }

    @Override
    protected Map<String, Function<String, BaseClientProperty>> getFunctionMap() {
        return map;
    }
}
