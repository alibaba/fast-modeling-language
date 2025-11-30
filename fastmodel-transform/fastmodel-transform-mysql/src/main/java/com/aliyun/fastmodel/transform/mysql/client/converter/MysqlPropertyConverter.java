package com.aliyun.fastmodel.transform.mysql.client.converter;

import java.util.Map;
import java.util.function.Function;

import com.aliyun.fastmodel.transform.api.client.converter.BasePropertyConverter;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.google.common.collect.Maps;

/**
 * MySQL属性转换器
 *
 * @author panguanjing
 * @date 2025/10/11
 */
public class MysqlPropertyConverter extends BasePropertyConverter {

    private static Map<String, Function<String, BaseClientProperty>> functionMap = Maps.newHashMap();

    public MysqlPropertyConverter() {
        init();
    }

    private void init() {
        // 初始化MySQL特定的属性转换函数
    }

    @Override
    public BaseClientProperty create(String name, String value) {
        return super.create(name, value);
    }

    @Override
    protected Map<String, Function<String, BaseClientProperty>> getFunctionMap() {
        return functionMap;
    }
}