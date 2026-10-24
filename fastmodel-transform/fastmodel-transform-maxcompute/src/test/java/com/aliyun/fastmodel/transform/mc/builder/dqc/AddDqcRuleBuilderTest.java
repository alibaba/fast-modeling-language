/*
 * Copyright 2021 Alibaba Group Holding Ltd.
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

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.dqc.AddDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.check.BaseCheckElement;
import com.aliyun.fastmodel.core.tree.statement.dqc.check.TableCheckElement;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/6/28
 */
public class AddDqcRuleBuilderTest {

    @Test
    public void build() {
        AddDqcRuleBuilder addRulesBuilder = new AddDqcRuleBuilder();
        BaseCheckElement build1 = TableCheckElement.builder().checkerName(new Identifier("name")).build();
        AddDqcRule addRules = new AddDqcRule(
            QualifiedName.of("dim_shop"),
            ImmutableList.of(),
            ImmutableList.of(build1)
        );
        DialectNode build = addRulesBuilder.build(addRules, MaxComputeContext.builder().build());
        assertNotNull(build);
    }
}