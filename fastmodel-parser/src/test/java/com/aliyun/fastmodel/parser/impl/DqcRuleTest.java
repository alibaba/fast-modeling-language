/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.parser.impl;

import com.aliyun.fastmodel.core.tree.statement.dqc.AddDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.ChangeDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.CreateDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.DropDqcRule;
import com.aliyun.fastmodel.parser.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * DqcRule test
 *
 * @author panguanjing
 * @date 2021/7/21
 */
public class DqcRuleTest extends BaseTest {

    @Test
    public void testCreateDqcRule() {
        String fml = "CREATE DQC_RULE a ON TABLE dim_shop (CONSTRAINT c CHECK(null_count(col1) =0) NOT ENFORCED)";
        CreateDqcRule parse = parse(fml, CreateDqcRule.class);
        assertEquals(parse.toString(), "CREATE DQC_RULE a ON TABLE dim_shop\n"
            + "(\n"
            + "   CONSTRAINT c CHECK(null_count(col1) = 0) NOT ENFORCED\n"
            + ")");
    }

    @Test
    public void testAddDqcRule() {
        String fml = "ALTER DQC_RULE ON TABLE dim_shop ADD (CONSTRAINT c CHECK(null_count(col1) =0) NOT ENFORCED)";
        AddDqcRule parse = parse(fml, AddDqcRule.class);
        assertEquals(parse.toString(), "ALTER DQC_RULE ON TABLE dim_shop\n"
            + "ADD (\n"
            + "   CONSTRAINT c CHECK(null_count(col1) = 0) NOT ENFORCED\n"
            + ")");
    }

    @Test
    public void testChangeDqcRule() {
        String fml
            = "ALTER DQC_RULE ON TABLE dim_shop CHANGE (c2 CONSTRAINT c CHECK(null_count(col1) =0) NOT ENFORCED)";
        ChangeDqcRule parse = parse(fml, ChangeDqcRule.class);
        assertEquals(parse.toString(), "ALTER DQC_RULE ON TABLE dim_shop\n"
            + "CHANGE (\n"
            + "   c2   CONSTRAINT c CHECK(null_count(col1) = 0) NOT ENFORCED\n"
            + ")");
    }

    @Test
    public void testChangeDqcRuleAbc() {
        String fml
            = "ALTER DQC_RULE ON TABLE dim_shop CHANGE (c2 CONSTRAINT c NOT ENFORCED)";
        ChangeDqcRule parse = parse(fml, ChangeDqcRule.class);
        assertEquals(parse.toString(), "ALTER DQC_RULE ON TABLE dim_shop\n"
            + "CHANGE (\n"
            + "   c2   CONSTRAINT c NOT ENFORCED\n"
            + ")");
    }

    @Test
    public void testDropRule() {
        String fml
            = "ALTER DQC_RULE ON TABLE dim_shop DROP c1";
        DropDqcRule parse = parse(fml, DropDqcRule.class);
        assertEquals(parse.toString(), "ALTER DQC_RULE ON TABLE dim_shop DROP c1");
    }

}
