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
import com.aliyun.fastmodel.core.tree.statement.rule.function.column.InTableFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.table.TableFunction;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * MaxCompute Sql Converter处理
 *
 * @author panguanjing
 * @date 2021/6/4
 */
public class MaxComputeSqlConverterTest {
    MaxComputeSqlConverter maxComputeSqlConverter = new MaxComputeSqlConverter();

    @Test
    public void testConverter() {
        MaxComputeContext build = MaxComputeContext.builder().build();
        String convert = maxComputeSqlConverter.convert(QualifiedName.of("dim_shop"),
            new InTableFunction(QualifiedName.of("shop_code"), QualifiedName.of("code_table"),
                new Identifier("value")), null, build);
        assertEquals(convert, "SELECT count(*)\n"
            + "FROM\n"
            + "  dim_shop\n"
            + "WHERE NOT (shop_code IN (SELECT value\n"
            + "FROM\n"
            + "  code_table\n"
            + "))\n");
    }

    @Test
    public void testConverterUniqueKey() {
        MaxComputeContext build = MaxComputeContext.builder().mcProject("autotest").mcSchema("s1").build();
        String convert = maxComputeSqlConverter.convert(QualifiedName.of("dim_shop"),
            new TableFunction(BaseFunctionName.UNIQUE,
                ImmutableList.of(new Identifier("c1"), new Identifier("c2"))), null, build);
        assertEquals(convert, "SELECT count(*)\n"
            + "FROM\n"
            + "  (\n"
            + "      SELECT\n"
            + "        c1\n"
            + "      , c2\n"
            + "      FROM\n"
            + "        autotest.s1.dim_shop\n"
            + "      GROUP BY c1,c2\n"
            + "      HAVING count(*) > 1\n"
            + "   )  t\n");
    }
}