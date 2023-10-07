package com.aliyun.fastmodel.transform.spark.format;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/2/18
 */
public class SparkPropertyKeyTest {

    @Test
    public void testGetByValue() {
        SparkPropertyKey sortedBy = SparkPropertyKey.getByValue("sorted_by");
        assertEquals(sortedBy, SparkPropertyKey.SORTED_BY);
        sortedBy = SparkPropertyKey.getByValue("not_exist");
        assertNull(sortedBy);
    }
}