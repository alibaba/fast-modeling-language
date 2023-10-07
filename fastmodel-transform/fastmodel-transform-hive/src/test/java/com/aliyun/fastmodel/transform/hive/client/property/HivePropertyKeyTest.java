package com.aliyun.fastmodel.transform.hive.client.property;

import com.aliyun.fastmodel.transform.hive.format.HivePropertyKey;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/2/7
 */
public class HivePropertyKeyTest {
    @Test
    public void testGetByValue() {
        HivePropertyKey byValue = HivePropertyKey.getByValue("hive.table_external");
        assertNotNull(byValue);
        HivePropertyKey notExist = HivePropertyKey.getByValue("notExist");
        assertNull(notExist);
    }
}