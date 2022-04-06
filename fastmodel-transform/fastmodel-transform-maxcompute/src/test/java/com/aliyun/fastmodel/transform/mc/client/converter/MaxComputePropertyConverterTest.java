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

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.mc.client.property.LifeCycle;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/8/5
 */
public class MaxComputePropertyConverterTest {
    MaxComputePropertyConverter maxComputePropertyConverter = new MaxComputePropertyConverter();

    @Test
    public void initFunctionMap() {
        Map<String, Function<String, BaseClientProperty>> functionMap = maxComputePropertyConverter.getFunctionMap();
        assertEquals(1, functionMap.size());
        Function<String, BaseClientProperty> stringBaseClientPropertyFunction = functionMap.get(LifeCycle.LIFECYCLE);
        assertNotNull(stringBaseClientPropertyFunction);
    }

}