/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.client.generator;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.transform.api.client.CodeGenerator;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorModelRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.client.generator.DefaultCodeGenerator;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hologres.client.converter.HologresPropertyConverter;
import com.aliyun.fastmodel.transform.hologres.client.property.BinLogTTL;
import com.aliyun.fastmodel.transform.hologres.client.property.EnableBinLogLevel;
import com.aliyun.fastmodel.transform.hologres.client.property.EnableBinLogLevel.BinLogLevel;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test set property
 *
 * @author panguanjing
 * @date 2022/7/12
 */
public class HologresCodeGeneratorSetColumnCommentTest {
    @Test
    public void testSetComment() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        List<Column> beforeColumn = ImmutableList.of(
            Column.builder()
                .name("c1")
                .dataType("text")
                .build()
        );
        List<Column> afterColumn = ImmutableList.of(
            Column.builder()
                .name("c1")
                .dataType("text")
                .comment("abc")
                .build()
        );
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .before(Table.builder().name("a").columns(beforeColumn).build())
            .after(Table.builder().name("a").columns(afterColumn).build())
            .config(TableConfig.builder().dialectMeta(DialectMeta.DEFAULT_HOLO).build())
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        String result =
            generate.getDialectNodes().stream().filter(DialectNode::isExecutable).map(DialectNode::getNode).collect(Collectors.joining("\n"));
        assertEquals(result, "COMMENT ON COLUMN a.c1 IS 'abc';");
    }

    @Test
    public void testSetCommentNull() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        List<Column> beforeColumn = ImmutableList.of(
            Column.builder()
                .name("c1")
                .comment("abc")
                .dataType("text")
                .build()
        );
        List<Column> afterColumn = ImmutableList.of(
            Column.builder()
                .name("c1")
                .dataType("text")
                .comment(null)
                .build()
        );
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .before(Table.builder().name("a").columns(beforeColumn).build())
            .after(Table.builder().name("a").columns(afterColumn).build())
            .config(TableConfig.builder().dialectMeta(DialectMeta.DEFAULT_HOLO).build())
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        String result =
            generate.getDialectNodes().stream().filter(DialectNode::isExecutable).map(DialectNode::getNode).collect(Collectors.joining("\n"));
        assertEquals(result, "COMMENT ON COLUMN a.c1 IS NULL;");
    }
}
