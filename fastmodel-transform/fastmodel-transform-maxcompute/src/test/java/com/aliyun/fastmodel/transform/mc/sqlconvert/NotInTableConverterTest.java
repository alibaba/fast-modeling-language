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
import com.aliyun.fastmodel.core.tree.statement.rule.function.column.InTableFunction;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/6/4
 */
public class NotInTableConverterTest {
    NotInTableConverter inTableConverter = new NotInTableConverter();

    @Test
    public void convert() {
        InTableFunction inTableFunction = new InTableFunction(
            QualifiedName.of("shop_code"),
            QualifiedName.of("code_table"),
            new Identifier("value")
        );
        MaxComputeContext build = MaxComputeContext.builder().mcProject("autotest").mcSchema("s1").build();
        String convert = inTableConverter.convert(QualifiedName.of("dim_shop"), inTableFunction, null, build);
        assertEquals(convert, "SELECT count(*)\n"
            + "FROM\n"
            + "  autotest.s1.dim_shop\n"
            + "WHERE NOT (shop_code IN (SELECT value\n"
            + "FROM\n"
            + "  autotest.s1.code_table\n"
            + "))\n");
    }

    @Test
    public void testConvertWithoutSchema() {
        InTableFunction inTableFunction = new InTableFunction(
            QualifiedName.of("shop_code"),
            QualifiedName.of("code_table"),
            new Identifier("value")
        );
        MaxComputeContext build = MaxComputeContext.builder().build();
        String convert = inTableConverter.convert(QualifiedName.of("dim_shop"), inTableFunction, null, build);
        assertEquals(convert, "SELECT count(*)\n"
            + "FROM\n"
            + "  dim_shop\n"
            + "WHERE NOT (shop_code IN (SELECT value\n"
            + "FROM\n"
            + "  code_table\n"
            + "))\n");
    }
}