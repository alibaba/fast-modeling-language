/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.example.integrate;

import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/6/17
 */
public class MaxComputeToHologresIntegrateTest {

    MaxComputeToHologresIntegrate maxComputeToHologresIntegrate;

    @Before
    public void setUp() throws Exception {
        maxComputeToHologresIntegrate = new MaxComputeToHologresIntegrate();
    }

    @Test
    public void testCase1() {
        String mc = "SELECT  *\n"
            + "FROM    project.test\n"
            + ";";
        DialectNode dialectNode = maxComputeToHologresIntegrate.mcToHologres(new DialectNode(mc));
        String expected = "SELECT\n"
            + "    *\n"
            + "FROM\n"
            + "    test";
        MatcherAssert.assertThat(dialectNode.getNode(), Matchers.equalToIgnoringWhiteSpace(expected));
    }

    @Test
    public void testProjectionFilterAndGrouping() {
        String mc = "SELECT a, MAX(b) AS largest_value FROM example_project.test WHERE b > 10 GROUP BY a";
        DialectNode actual = maxComputeToHologresIntegrate.mcToHologres(new DialectNode(mc));
        String expected = "SELECT a , MAX(b) AS largest_value FROM test WHERE b > 10 GROUP BY a";
        MatcherAssert.assertThat(actual.getNode(), Matchers.equalToIgnoringWhiteSpace(expected));
    }
}