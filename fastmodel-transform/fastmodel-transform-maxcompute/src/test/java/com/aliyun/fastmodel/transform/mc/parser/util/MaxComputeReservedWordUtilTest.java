package com.aliyun.fastmodel.transform.mc.parser.util;

import java.util.Set;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/2/20
 */
public class MaxComputeReservedWordUtilTest {

    @Test
    public void testContainWord() {
        boolean reservedKeyWord = MaxComputeReservedWordUtil.isReservedKeyWord("double");
        assertTrue(reservedKeyWord);
        reservedKeyWord = MaxComputeReservedWordUtil.isReservedKeyWord("abc");
        assertFalse(reservedKeyWord);
    }
}