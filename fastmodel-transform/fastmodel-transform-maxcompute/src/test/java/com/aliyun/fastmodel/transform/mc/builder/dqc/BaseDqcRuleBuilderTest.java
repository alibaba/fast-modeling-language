/*
 * Copyright 2022 Alibaba Group Holding Ltd.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
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