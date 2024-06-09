package com.aliyun.fastmodel.transform.starrocks.client.converter;

import java.util.Map;
import java.util.function.Function;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.starrocks.format.StarRocksProperty;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/9/16
 */
public class StarRocksPropertyConverterTest {

    StarRocksPropertyConverter starRocksPropertyConverter = new StarRocksPropertyConverter();

    @Test
    public void getFunctionMap() {
        Map<String, Function<String, BaseClientProperty>> functionMap = starRocksPropertyConverter.getFunctionMap();
        Function<String, BaseClientProperty> stringBaseClientPropertyFunction = functionMap.get(StarRocksProperty.TABLE_INDEX_COMMENT.getValue());
        assertNotNull(stringBaseClientPropertyFunction);
        stringBaseClientPropertyFunction = functionMap.get(StarRocksProperty.TABLE_INDEX_TYPE.getValue());
        assertNotNull(stringBaseClientPropertyFunction);
    }
}