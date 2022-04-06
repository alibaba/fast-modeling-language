/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.mc.builder.dqc;

import java.util.List;

import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.rule.PartitionSpec;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/10/25
 */
public class BaseDqcRuleBuilderTest {

    BaseDqcRuleBuilder dqcRuleBuilder = new DropDqcRuleBuilder();
    @Test
    public void toMatchExpression() {
        List<PartitionSpec> list = Lists.newArrayList();
        PartitionSpec spec = new PartitionSpec(new Identifier("ds"), new StringLiteral("yyyymmdd"));
        list.add(spec);
        spec = new PartitionSpec(new Identifier("region"), new StringLiteral("beijing,nanjing"));
        list.add(spec);
        String expression = dqcRuleBuilder.toMatchExpression(list);
        assertEquals(expression, "ds='yyyymmdd'/region='beijing,nanjing'");
    }
}