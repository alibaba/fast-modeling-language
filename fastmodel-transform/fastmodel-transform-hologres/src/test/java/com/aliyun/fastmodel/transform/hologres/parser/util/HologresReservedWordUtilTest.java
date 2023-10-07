package com.aliyun.fastmodel.transform.hologres.parser.util;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/1/27
 */
public class HologresReservedWordUtilTest {

    @Test
    public void isReservedKeyWord() {
        boolean array = HologresReservedWordUtil.isReservedKeyWord("array");
        assertTrue(array);

        boolean abc = HologresReservedWordUtil.isReservedKeyWord("abc");
        assertFalse(abc);
    }
}