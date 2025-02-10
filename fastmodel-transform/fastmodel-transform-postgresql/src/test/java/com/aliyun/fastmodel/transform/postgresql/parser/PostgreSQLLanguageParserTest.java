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

package com.aliyun.fastmodel.transform.postgresql.parser;

import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import org.junit.Test;

import static org.junit.Assert.assertNull;

/**
 * Desc:
 * <a href="https://www.postgresql.org/docs/current/sql-createtable.html">create table</a>
 *
 * @author panguanjing
 * @date 2024/10/13
 */
public class PostgreSQLLanguageParserTest {

    PostgreSQLLanguageParser parser = new PostgreSQLLanguageParser();

    @Test
    public void parseNode() {
        CreateTable o = parser.parseNode("CREATE TABLE films (\n"
            + "    code        char(5) CONSTRAINT firstkey PRIMARY KEY,\n"
            + "    title       varchar(40) NOT NULL,\n"
            + "    did         integer NOT NULL,\n"
            + "    date_prod   date,\n"
            + "    kind        varchar(10),\n"
            + "    len         interval hour to minute\n"
            + ");\n");
        assertNull(o);
    }
}