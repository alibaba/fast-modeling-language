/*
 * Copyright [2024] [name of copyright owner]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.fastmodel.transform.adbmysql.client;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.adbmysql.AdbMysqlTransformer;
import com.aliyun.fastmodel.transform.adbmysql.context.AdbMysqlTransformContext;
import com.aliyun.fastmodel.transform.api.client.CodeGenerator;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorModelRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.client.generator.DefaultCodeGenerator;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * <a href="https://help.aliyun.com/zh/analyticdb-for-mysql/use-cases/schema-design?spm=a2c4g.11186623.0.0.47563f47boqajz#section-4s5-di5-bjt">...</a>
 * adb mysql generator
 *
 * @author panguanjing
 * @date 2024/3/19
 */
public class AdbMysqlGeneratorTest extends BaseAdbMysqlTest {

    CodeGenerator codeGenerator = new DefaultCodeGenerator();

    AdbMysqlTransformer adbMysqlTransformer = new AdbMysqlTransformer();

    @Test
    public void testGenerator() {
        assertGeneratorWithFile("simple.txt", "simple_result.txt");
    }

    private void assertGeneratorWithFile(String file, String file1) {
        assertGenerator(file, getText(file1));
    }

    @Test
    public void testGeneratorWithDateFormat() {
        assertGeneratorWithFile("adbmysql_date_format.txt", "adbmysql_date_format_result.txt");
    }

    private void assertGenerator(String file, String expect) {
        String text = getText(file);
        DialectNode dialectNode = new DialectNode(text);
        BaseStatement reverse = adbMysqlTransformer.reverse(dialectNode);
        Table table = adbMysqlTransformer.transformTable(reverse, AdbMysqlTransformContext.builder().build());
        assertText(expect, table);
    }

    private void assertText(String expect, Table table) {
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .config(TableConfig.builder()
                .dialectMeta(DialectMeta.DEFAULT_ADB_MYSQL)
                .build())
            .after(table)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        assertEquals(expect, generate.getDialectNodes().get(0).getNode());
    }
}
