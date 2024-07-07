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
import java.util.List;

import com.aliyun.fastmodel.compare.CompareNodeExecute;
import com.aliyun.fastmodel.compare.CompareStrategy;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hologres.dialect.HologresVersion;
import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * 修改表名的对比测试
 *
 * @author panguanjing
 * @date 2024/4/11
 */
public class Issue6Test extends BaseIssueTest {

    @Test
    @SneakyThrows
    public void testRenameCompare() {
        String text = IOUtils.resourceToString("/hologres/rename.txt", Charset.defaultCharset());
        String generator = this.generator(text);
        assertEquals("BEGIN;\n"
            + "CREATE TABLE public.test_2 (\n"
            + "   c1 TEXT NOT NULL,\n"
            + "   c2 TEXT,\n"
            + "   c3 TEXT,\n"
            + "   c4 TEXT,\n"
            + "   PRIMARY KEY(c1)\n"
            + ");\n"
            + "CALL SET_TABLE_PROPERTY('public.test_2', 'dictionary_encoding_columns', '\"c1\":auto,\"c3\":auto,\"c2\":auto');\n"
            + "COMMIT;", generator);
    }

    @Test
    @SneakyThrows
    public void testPropertyCompare() {
        String before = IOUtils.resourceToString("/hologres/rename.txt", Charset.defaultCharset());
        String after = IOUtils.resourceToString("/hologres/rename2.txt", Charset.defaultCharset());
        ReverseContext reverse = ReverseContext.builder().merge(true).version(HologresVersion.V2).build();
        BaseStatement beforeStatement = hologresTransformer.reverse(new DialectNode(before), reverse);
        BaseStatement afterStatement = hologresTransformer.reverse(new DialectNode(after), reverse);
        //比较node
        List<BaseStatement> compare = CompareNodeExecute.getInstance().compare(beforeStatement, afterStatement, CompareStrategy.INCREMENTAL);
        assertEquals(0, compare.size());
    }
}
