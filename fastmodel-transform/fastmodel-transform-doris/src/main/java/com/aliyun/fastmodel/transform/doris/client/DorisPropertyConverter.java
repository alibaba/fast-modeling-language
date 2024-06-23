package com.aliyun.fastmodel.transform.doris.client;

import java.util.Map;
import java.util.function.Function;

import com.aliyun.fastmodel.transform.api.client.converter.BasePropertyConverter;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.google.common.collect.Maps;

/**
 * doris property converter
 *
 * @author panguanjing
 * @date 2024/1/21
 */
public class DorisPropertyConverter extends BasePropertyConverter {

    private static Map<String, Function<String, BaseClientProperty>> functionMap = Maps.newHashMap();

    public DorisPropertyConverter() {
        init();
    }

    private void init() {

    }

    @Override
    protected Map<String, Function<String, BaseClientProperty>> getFunctionMap() {
        return functionMap;
    }
}
