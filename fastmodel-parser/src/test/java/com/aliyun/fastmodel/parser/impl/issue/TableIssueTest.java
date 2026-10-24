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

package com.aliyun.fastmodel.parser.impl.issue;

import com.aliyun.fastmodel.core.exception.SemanticException;
import com.aliyun.fastmodel.core.parser.DomainLanguage;
import com.aliyun.fastmodel.core.parser.FastModelParser;
import com.aliyun.fastmodel.core.parser.FastModelParserFactory;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.parser.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/4/13
 */
public class TableIssueTest extends BaseTest {

    @Test
    public void testParseTable() {
        String fmlSql = "CREATE DIM TABLE IF NOT EXISTS example.dim_store_mapping \n"
            + "(\n"
            + "   region_code string COMMENT 'Region code',\n"
            + "   region_name string COMMENT 'Region name',\n"
            + "   store_code string COMMENT 'Store code' WITH ('dict'='store_code'),\n"
            + "   store_name string COMMENT 'Store name' WITH ('dict'='store_name')\n"
            + ")\n"
            + " COMMENT 'Example region and store mapping'\n"
            + " PARTITIONED BY (ds string COMMENT 'Month in YYYYMM format')\n"
            + " WITH ('business_process'='example_process')";
        FastModelParser fastModelParser = FastModelParserFactory.getInstance().get();
        assertNotNull(fastModelParser.parse(new DomainLanguage(fmlSql)));
    }

    @Test
    public void testParseTableWithStruct() {
        // Synthetic fixtures retain nested maps, arrays, structs and quoted numeric fields.
        String fml = "CREATE FACT TABLE example.fact_event ALIAS 'Example events'\n"
            + "(\n"
            + "event_id BIGINT COMMENT 'Event identifier'\n"
            + "    ,attributes MAP<STRING\n"
            + "    ,STRING> COMMENT 'Event attributes'\n"
            + "    ,steps ARRAY<STRUCT<step_id:BIGINT\n"
            + "    ,created_at:STRING\n"
            + "    ,state:BIGINT\n"
            + "    ,labels:MAP<STRING\n"
            + "    ,STRING>>> COMMENT 'Nested step records'\n"
            + "    ,changes ARRAY<STRUCT<step_id:BIGINT\n"
            + "    ,change_id:BIGINT\n"
            + "    ,changed_at:STRING\n"
            + "    ,properties:MAP<STRING\n"
            + "    ,STRING>>> COMMENT 'Nested change records'\n"
            + "    ,timestamps ARRAY<STRING> COMMENT 'Event timestamps'\n"
            + "    ,samples ARRAY<STRUCT<`0`:STRING\n"
            + "    ,`1`:STRING\n"
            + "    ,`2`:STRING\n"
            + "    ,`3`:STRING>> COMMENT 'Sample tuple fields'\n"
            + ")\n"
            + "PARTITIONED BY\n"
            + "(\n"
            + "   ds ALIAS 'Event date' STRING COMMENT 'Date in yyyymmdd format'\n"
            + ")\n"
            + "WITH('life_cycle'='1');";

        CreateTable createTable = nodeParser.parseStatement(fml);
        assertEquals(6, createTable.getColumnDefines().size());
    }

    @Test(expected = SemanticException.class)
    public void testMultiPrimaryKey() {
        String fml = "CREATE ADVANCED DWS TABLE IF NOT EXISTS example_event_summary\n"
            + "(category_id ALIAS 'Category id' bigint PRIMARY KEY COMMENT 'Category id' REFERENCES category.id\n"
            + ",category_level ALIAS 'Category level' string PRIMARY KEY COMMENT 'Category level' "
            + "REFERENCES category.level\n"
            + ",event_count ALIAS 'Event count' bigint MEASUREMENT COMMENT 'Event count' "
            + "WITH ('atomic_indicator' = 'event_count')\n"
            + ", CONSTRAINT c0 TIME_PERIOD KEY REFERENCES (`1d`,`ftd`,`itd`))\n"
            + " COMMENT 'Example event summary'\n"
            + " PARTITIONED BY (ds ALIAS 'Date' string COMMENT 'Date', "
            + "group_id ALIAS 'Group id' string COMMENT 'Group id' REFERENCES groups.id)\n"
            + " WITH ('business_category' = 'example.events', 'data_layer' = 'example_layer');";
        nodeParser.parseStatement(fml);
    }
}
