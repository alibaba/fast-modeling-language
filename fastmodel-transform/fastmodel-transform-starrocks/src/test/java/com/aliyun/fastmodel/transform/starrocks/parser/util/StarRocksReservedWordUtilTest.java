package com.aliyun.fastmodel.transform.starrocks.parser.util;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/11/6
 */
public class StarRocksReservedWordUtilTest {

    @Test
    public void isReservedKeyWord() {
        boolean reservedKeyWord = StarRocksReservedWordUtil.isReservedKeyWord("add");
        assertTrue(reservedKeyWord);
    }
}