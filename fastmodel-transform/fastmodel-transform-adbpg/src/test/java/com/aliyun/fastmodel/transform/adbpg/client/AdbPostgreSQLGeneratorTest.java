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

package com.aliyun.fastmodel.transform.adbpg.client;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.transform.api.client.CodeGenerator;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorModelRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.client.generator.DefaultCodeGenerator;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author 云异
 * @date 2025/2/18
 */
public class AdbPostgreSQLGeneratorTest {

    @Test
    public void testDiffTable() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        Table before = Table.builder()
            .name("test")
            .columns(
                Lists.newArrayList(Column.builder().name("id").dataType("INT").build())
            ).build();
        Table after = Table.builder()
            .name("test")
            .columns(
                Lists.newArrayList(Column.builder().name("id").dataType("INT").build(),
                    Column.builder().name("name").dataType("VARCHAR").comment("haha").build())
            ).build();
        TableConfig tableConfig = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_ADB_PG)
            .caseSensitive(false)
            .build();
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .before(before)
            .after(after)
            .config(tableConfig)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals("ALTER TABLE test ADD COLUMN name VARCHAR;\n"
            + "COMMENT ON COLUMN test.name IS 'haha';", dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining(",\n")));
    }

}
