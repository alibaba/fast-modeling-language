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
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * issue5 text
 * default value issue
 *
 * @author panguanjing
 * @date 2024/4/11
 */
public class Issue5Text extends BaseIssueTest {

    @Test
    @SneakyThrows
    public void testGeneratorDefault() {
        String text = IOUtils.resourceToString("/hologres/default_text.txt", Charset.defaultCharset());
        String generator = this.generator(text);
        assertEquals("BEGIN;\n"
            + "CREATE TABLE public.table_name11 (\n"
            + "   c1 TEXT PRIMARY KEY DEFAULT '1',\n"
            + "   c2 TEXT DEFAULT '2',\n"
            + "   c3 TEXT DEFAULT '3',\n"
            + "   c4 INTEGER DEFAULT 1\n"
            + ") PARTITION BY LIST(c1);\n"
            + "\n"
            + "COMMIT;", generator);
    }

    @SneakyThrows
    @Test
    public void testGeneratorDefaultValueWithTable() {
        String text = IOUtils.resourceToString("/hologres/default_text.txt", Charset.defaultCharset());
        BaseStatement reverse = hologresTransformer.reverse(new DialectNode(text), ReverseContext.builder().merge(true).build());
        Table table = hologresTransformer.transformTable(reverse, TransformContext.builder().build());
        Column column = table.getColumns().get(0);
        String defaultValue = column.getDefaultValue();
        assertEquals("1", defaultValue);
    }
}
