package com.aliyun.fastmodel.transform.doris.parser.util;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/1/24
 */
public class DorisReservedWordUtilTest {

    @Test
    public void isReservedKeyWord() {
        boolean flag = DorisReservedWordUtil.isReservedKeyWord("add");
        assertTrue(flag);
        flag = DorisReservedWordUtil.isReservedKeyWord("test");
        assertFalse(flag);
    }
}