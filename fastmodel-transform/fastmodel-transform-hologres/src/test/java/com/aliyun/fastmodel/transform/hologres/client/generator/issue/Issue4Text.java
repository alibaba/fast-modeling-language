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

package com.aliyun.fastmodel.transform.hologres.client.generator.issue;

import java.nio.charset.Charset;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.client.CodeGenerator;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorModelRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.client.generator.DefaultCodeGenerator;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hologres.HologresTransformer;
import com.aliyun.fastmodel.transform.hologres.dialect.HologresVersion;
import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/3/27
 */
public class Issue4Text {
    HologresTransformer hologresTransformer = new HologresTransformer();

    @Test
    @SneakyThrows
    public void testAutoColumn() {
        String text = IOUtils.resourceToString("/hologres/auto.txt", Charset.defaultCharset());
        BaseStatement reverse = hologresTransformer.reverse(new DialectNode(text), ReverseContext.builder().version(HologresVersion.V2).merge(true)
            .build());
        Table table = hologresTransformer.transformTable(reverse, TransformContext.builder().build());
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .after(table)
            .config(TableConfig.builder().dialectMeta(DialectMeta.getByNameAndVersion(DialectName.HOLOGRES.getValue(), HologresVersion.V2)).build())
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        DialectNode dialectNode = generate.getDialectNodes().get(0);
        assertEquals("BEGIN;\n"
            + "CREATE TABLE IF NOT EXISTS public.hologres_parent (\n"
            + "   a TEXT,\n"
            + "   b INTEGER,\n"
            + "   c TIMESTAMP,\n"
            + "   d TEXT\n"
            + ") PARTITION BY LIST(a);\n"
            + "CALL SET_TABLE_PROPERTY('public.hologres_parent', 'time_to_live_in_seconds', '3153600000');\n"
            + "CALL SET_TABLE_PROPERTY('public.hologres_parent', 'orientation', 'column');\n"
            + "CALL SET_TABLE_PROPERTY('public.hologres_parent', 'binlog.level', 'none');\n"
            + "CALL SET_TABLE_PROPERTY('public.hologres_parent', 'bitmap_columns', '\"a\",\"d\"');\n"
            + "CALL SET_TABLE_PROPERTY('public.hologres_parent', 'dictionary_encoding_columns', '\"a\":auto,\"d\":auto');\n"
            + "COMMIT;", dialectNode.getNode());
    }
}
