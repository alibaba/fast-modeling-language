package com.aliyun.fastmodel.transform.hologres.parser.util;

import com.aliyun.fastmodel.transform.hologres.client.property.HoloPropertyKey;
import com.aliyun.fastmodel.transform.hologres.dialect.HologresVersion;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/4/16
 */
public class HologresPropertyUtilTest {

    @Test
    public void getPropertyValue() {
        String value = HologresPropertyUtil.getPropertyValue(HologresVersion.V1,
            HoloPropertyKey.DICTIONARY_ENCODING_COLUMN.getValue(), "c1:auto,c2");
        assertEquals("\"c1:auto,c2\"", value);

        value = HologresPropertyUtil.getPropertyValue(HologresVersion.V2,
            HoloPropertyKey.DICTIONARY_ENCODING_COLUMN.getValue(), "c1:auto,c2");
        assertEquals("\"c1\":auto,\"c2\":auto", value);
    }
}