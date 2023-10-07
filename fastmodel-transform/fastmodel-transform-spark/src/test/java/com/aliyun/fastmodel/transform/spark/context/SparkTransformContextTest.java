package com.aliyun.fastmodel.transform.spark.context;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/2/18
 */
public class SparkTransformContextTest {

    @Test
    public void testContext() {
        SparkTransformContext build = SparkTransformContext.builder().build();
        assertEquals(build.getSparkTableFormat(), SparkTableFormat.HIVE_FORMAT);
        SparkTransformContext build1 = SparkTransformContext.builder().tableFormat(SparkTableFormat.DATASOURCE_FORMAT).build();
        assertEquals(build1.getSparkTableFormat(), SparkTableFormat.DATASOURCE_FORMAT);
    }
}