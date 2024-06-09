package com.aliyun.fastmodel.transform.starrocks.client.constraint;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/12/13
 */
public class StarRocksConstraintTypeTest {

    @Test
    public void testGetByValue() {
        StarRocksConstraintType primaryKey = StarRocksConstraintType.getByValue("primary");
        assertEquals(primaryKey, StarRocksConstraintType.PRIMARY_KEY);

        StarRocksConstraintType unique = StarRocksConstraintType.getByValue("unique");
        assertEquals(unique, StarRocksConstraintType.UNIQUE_KEY);
    }
}