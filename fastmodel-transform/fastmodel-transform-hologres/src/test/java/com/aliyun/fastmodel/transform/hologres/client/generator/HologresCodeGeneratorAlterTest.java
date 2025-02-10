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

package com.aliyun.fastmodel.transform.hologres.client.generator;

import java.nio.charset.Charset;

import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.api.Transformer;
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
import com.aliyun.fastmodel.transform.hologres.HologresV3Transformer;
import com.aliyun.fastmodel.transform.hologres.dialect.HologresVersion;
import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * alter test
 *
 * @author panguanjing
 * @date 2024/10/9
 */
public class HologresCodeGeneratorAlterTest {

    HologresV3Transformer hologresV3Transformer = new HologresV3Transformer();

    CodeGenerator codeGenerator = new DefaultCodeGenerator();

    @Test
    public void testAlterDynamicNoColumn() {
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        String beforeText = getText("dynamic_no_column_before.txt");
        Table before = generator(new DialectNode(beforeText), hologresV3Transformer);
        request.setBefore(before);
        String afterText = getText("dynamic_no_column_after.txt");
        Table after = generator(new DialectNode(afterText), hologresV3Transformer);
        request.setAfter(after);
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.getByNameAndVersion(DialectName.HOLOGRES.getValue(), HologresVersion.V3)).build();
        request.setConfig(config);
        DdlGeneratorResult generate = codeGenerator.generate(request);
        assertEquals("BEGIN;\n"
            + "ALTER TABLE b.sales_incremental SET task_definition = $_dataworks_system_$\n"
            + "SELECT day, sum(amount), count(1)\n"
            + "    FROM base_sales\n"
            + "  GROUP BY day\n"
            + "$_dataworks_system_$;\n"
            + "ALTER TABLE b.sales_incremental SET (refresh_mode='streaming',auto_refresh_enable='true');\n"
            + "COMMIT;", generate.getDialectNodes().get(0).getNode());
    }

    private Table generator(DialectNode dialectNode, Transformer transformer) {
        ReverseContext build = ReverseContext.builder().merge(true).build();
        Node node = transformer.reverse(dialectNode, build);
        return transformer.transformTable(node, TransformContext.builder().build());
    }

    @SneakyThrows
    private String getText(String path) {
        return IOUtils.resourceToString("/hologres/dynamic/alter/" + path, Charset.defaultCharset());
    }
}
