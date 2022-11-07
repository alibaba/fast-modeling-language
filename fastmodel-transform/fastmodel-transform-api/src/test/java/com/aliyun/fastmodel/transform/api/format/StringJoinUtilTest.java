/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.format;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.transform.api.util.StringJoinUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * tableName Util test
 *
 * @author panguanjing
 * @date 2022/5/20
 */
public class StringJoinUtilTest {

    @Test
    public void testTableName() {
        QualifiedName join = StringJoinUtil.join("a", "b", "c");
        assertEquals(join.toString(), "a.b.c");
    }

    @Test
    public void testSuffix() {
        QualifiedName join = StringJoinUtil.join("", "", "c");
        assertEquals(join.toString(), "c");
    }

    @Test
    public void testJoinTwo() {
        QualifiedName join = StringJoinUtil.join("a", "", "c");
        assertEquals(join.toString(), "a.c");
    }

    @Test
    public void testJoinTwoWay() {
        QualifiedName join = StringJoinUtil.join("", "b", "c");
        assertEquals(join.toString(), "b.c");
    }

    @Test(expected = AssertionError.class)
    public void testJoinOne() {
        QualifiedName join = StringJoinUtil.join("a", null, "");
        assertEquals(join.toString(), "a");
    }
}
