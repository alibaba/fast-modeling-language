package com.aliyun.fastmodel.transform.starrocks.client.converter;

import java.util.Map;
import java.util.function.Function;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import org.junit.Test;

import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.TABLE_INDEX_COMMENT;
import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.TABLE_INDEX_TYPE;
import static org.junit.Assert.assertNotNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/9/16
 */
public class StarRocksPropertyKeyConverterTest {

    StarRocksPropertyConverter starRocksPropertyConverter = new StarRocksPropertyConverter();

    @Test
    public void getFunctionMap() {
        Map<String, Function<String, BaseClientProperty>> functionMap = starRocksPropertyConverter.getFunctionMap();
        Function<String, BaseClientProperty> stringBaseClientPropertyFunction = functionMap.get(TABLE_INDEX_COMMENT.getValue());
        assertNotNull(stringBaseClientPropertyFunction);
        stringBaseClientPropertyFunction = functionMap.get(TABLE_INDEX_TYPE.getValue());
        assertNotNull(stringBaseClientPropertyFunction);
    }
}