/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.parser.impl;

import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.tree.statement.command.ExportSql;
import com.aliyun.fastmodel.core.tree.statement.command.FormatFml;
import com.aliyun.fastmodel.core.tree.statement.command.ImportSql;
import com.aliyun.fastmodel.parser.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * 针对command命令的测试
 *
 * @author panguanjing
 * @date 2021/12/1
 */
public class CommandTest extends BaseTest {

    @Test
    public void testImport() {
        String fml = "IMP_SQL -m mysql -t 'hello';";
        ImportSql parse = parse(fml, ImportSql.class);
        assertEquals(parse.getDialect().getValue(), "mysql");
        assertEquals(parse.getSql().get(), "hello");
    }

    @Test
    public void testExport() {
        String fml = "EXP_SQL -m mysql -t 'create dim table a'";
        ExportSql sqlCommand = parse(fml, ExportSql.class);
        assertEquals(sqlCommand.getDialect().getValue(), "mysql");
        assertEquals(sqlCommand.getFml().get(), "create dim table a");
    }

    @Test
    public void rejectsUriOptionsForImportAndExport() {
        String[] commands = {"IMP_SQL", "IMPORT_SQL", "EXP_SQL", "EXPORT_SQL"};
        String[] locations = {"file:///tmp/example.sql", "http://example.invalid/example.sql",
            "https://example.invalid/example.sql", ""};
        for (String command : commands) {
            for (String location : locations) {
                String fml = command + " -m mysql -u '" + location + "'";
                try {
                    parse(fml, Object.class);
                    fail("URI options must be rejected: " + fml);
                } catch (ParseException expected) {
                    // URI commands must be rejected during parsing, before any I/O is possible.
                }
            }
        }
    }

    @Test
    public void testFormat() {
        String fml = "format -m fml -t 'create dim table a (b bigint) comment \"abc\" ' ";
        FormatFml formatFml = parse(fml, FormatFml.class);
        assertEquals(formatFml.getFml().get(), "create dim table a (b bigint) comment \"abc\" ");
    }

}

