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

package com.aliyun.fastmodel.transform.mc.sqlconvert;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunctionName;
import com.aliyun.fastmodel.core.tree.statement.rule.function.table.TableFunction;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/6/7
 */
public class UniqueKeyConverterTest {

    @Test
    public void testConvert() {
        MaxComputeContext build = MaxComputeContext.builder().mcProject("autotest").mcSchema("s1").build();
        UniqueKeyConverter converter = new UniqueKeyConverter();
        String convert = converter.convert(QualifiedName.of("dim_shop"),
            new TableFunction(BaseFunctionName.UNIQUE, ImmutableList.of(
                new Identifier("name"),
                new Identifier("code")
            )), null, build);
        assertEquals("SELECT count(*)\n"
            + "FROM\n"
            + "  (\n"
            + "      SELECT\n"
            + "        name\n"
            + "      , code\n"
            + "      FROM\n"
            + "        autotest.s1.dim_shop\n"
            + "      GROUP BY name,code\n"
            + "      HAVING count(*) > 1\n"
            + "   )  t\n", convert);
    }

    @Test
    public void testConvertWithMatchExpression() {
        UniqueKeyConverter converter = new UniqueKeyConverter();
        MaxComputeContext build = MaxComputeContext.builder().mcProject("autotest").mcSchema("s1").build();
        String convert = converter.convert(QualifiedName.of("dim_shop"),
            new TableFunction(BaseFunctionName.UNIQUE, ImmutableList.of(
                new Identifier("name"),
                new Identifier("code")
            )), "ds=<yyyy-mm-dd>", build);
        assertEquals("SELECT count(*)\n"
            + "FROM\n"
            + "  (\n"
            + "      SELECT\n"
            + "        name\n"
            + "      , code\n"
            + "      FROM\n"
            + "        autotest.s1.dim_shop\n"
            + "      WHERE $dqc\n"
            + "      GROUP BY name,code\n"
            + "      HAVING count(*) > 1\n"
            + "   )  t\n", convert);
    }

}