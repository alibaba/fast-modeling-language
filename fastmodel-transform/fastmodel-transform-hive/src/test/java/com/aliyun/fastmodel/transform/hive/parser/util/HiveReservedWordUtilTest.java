package com.aliyun.fastmodel.transform.hive.parser.util;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/2/20
 */
public class HiveReservedWordUtilTest {

    @Test
    public void isReservedKeyWord() {
        boolean reservedKeyWord = HiveReservedWordUtil.isReservedKeyWord("double");
        assertTrue(reservedKeyWord);
        reservedKeyWord = HiveReservedWordUtil.isReservedKeyWord("abc");
        assertFalse(reservedKeyWord);
    }

    @Test
    public void testReservedKeyWord() {
        boolean reservedKeyWord = HiveReservedWordUtil.isReservedKeyWord("int");
        assertTrue(reservedKeyWord);
        reservedKeyWord = HiveReservedWordUtil.isReservedKeyWord("decimal");
        assertTrue(reservedKeyWord);
        reservedKeyWord = HiveReservedWordUtil.isReservedKeyWord("integer");
        assertTrue(reservedKeyWord);
    }
}