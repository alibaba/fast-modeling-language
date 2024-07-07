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
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hologres.HologresTransformer;
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
public class Issue3Text {
    HologresTransformer hologresTransformer = new HologresTransformer();

    @Test
    @SneakyThrows
    public void testPrimaryKey() {
        String text = IOUtils.resourceToString("/hologres/primary.txt", Charset.defaultCharset());
        BaseStatement reverse = hologresTransformer.reverse(new DialectNode(text));
        Table table = hologresTransformer.transformTable(reverse, TransformContext.builder().build());
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .after(table)
            .config(TableConfig.builder().dialectMeta(DialectMeta.DEFAULT_HOLO).build())
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        DialectNode dialectNode = generate.getDialectNodes().get(0);
        assertEquals("BEGIN;\n"
            + "CREATE TABLE holo_test (\n"
            + "   id     INTEGER NOT NULL,\n"
            + "   \"name\" TEXT,\n"
            + "   PRIMARY KEY(id)\n"
            + ");\n"
            + "\n"
            + "COMMIT;", dialectNode.getNode());
    }
}
