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

package com.aliyun.fastmodel.transform.mc.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.transform.api.client.CodeGenerator;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.property.StringProperty;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorSqlRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.client.generator.DefaultCodeGenerator;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * code generator
 *
 * @author panguanjing
 * @date 2022/6/10
 */
public class MaxComputeCodeGeneratorSqlTest {

    @Test
    public void testGeneratorWithDrop() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .caseSensitive(false)
            .build();
        Table build = Table.builder().database("a").schema("s").name("a").build();
        DdlGeneratorSqlRequest request = DdlGeneratorSqlRequest.builder()
            .after(build)
            .config(config)
            .before("create table a (name string) comment 'c';")
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals(dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining(";\n")), "DROP TABLE IF EXISTS a.s.a;");
    }

    @Test
    public void testGeneratorWithTableFormatVersionProperty() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .caseSensitive(false)
            .build();

        // Create a column
        Column column = Column.builder()
            .name("id")
            .dataType("BIGINT")
            .build();

        // Create properties
        List<BaseClientProperty> baseClientProperties = new ArrayList<>();
        StringProperty stringProperty = new StringProperty();
        stringProperty.setKey("table.format.version");
        stringProperty.setValueString("2");
        baseClientProperties.add(stringProperty);

        Table table = Table.builder()
            .database("project1")
            .name("tbl1")
            .columns(Arrays.asList(column))
            .properties(baseClientProperties)
            .build();

        DdlGeneratorSqlRequest request = DdlGeneratorSqlRequest.builder()
            .after(table)
            .config(config)
            .build();

        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();

        // Verify that the generated DDL includes the table.format.version property
        String ddl = dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining(";\n"));
        assertTrue("Generated DDL should contain table.format.version property", ddl.contains("table.format.version"));
        assertTrue("Generated DDL should contain the value '2'", ddl.contains("2"));
    }

}
