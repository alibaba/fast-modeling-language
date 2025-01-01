package com.aliyun.fastmodel.transform.flink.client.converter;

import java.util.Map;
import java.util.function.Function;

import com.aliyun.fastmodel.transform.api.client.converter.BasePropertyConverter;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.google.common.collect.Maps;

/**
 * @author 子梁
 * @date 2024/5/22
 */
public class FlinkPropertyConverter extends BasePropertyConverter {

    private static Map<String, Function<String, BaseClientProperty>> functionMap = Maps.newHashMap();

    public FlinkPropertyConverter() {
        init();
    }

    private void init() {

    }

    @Override
    protected Map<String, Function<String, BaseClientProperty>> getFunctionMap() {
        return functionMap;
    }
}
