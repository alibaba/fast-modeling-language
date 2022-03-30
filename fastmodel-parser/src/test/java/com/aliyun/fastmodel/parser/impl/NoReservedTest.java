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

import java.util.List;

import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.parser.BaseTest;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/4/9
 */
public class NoReservedTest extends BaseTest {

    @Test
    public void testNonReserved() {
        List<String> nonReserved = Lists.newArrayList(
            "url",
            "floor",
            "target_type",
            "domain",
            "grouping",
            "password",
            "system",
            "checker",
            "order",
            "date",
            "timestamp",
            "filter",
            "all",
            "business_process",
            //"cube",
            "dim",
            "fact",
            "like",
            "natural",
            "respect",
            "as",
            "bigint",
            "create",
            "current_timestamp",
            "except",
            "time_period",
            "cast",
            "current_date",
            "extract",
            "false",
            "interval",
            "null",
            "rows",
            "float",
            "on",
            "or",
            "to",
            "by",
            "char",
            "div",
            "boolean",
            "groups",
            "outer",
            "row",
            //"select",
            "true",
            "values",
            "constraint",
            "dayofweek",
            "transform",
            "window",
            "alter",
            "array",
            "current",
            "decimal",
            "full",
            "left",
            "percent",
            "rollup",
            "time_periods",
            "from",
            "ignore",
            "inner",
            "int",
            "is",
            "measure_unit",
            "over",
            "binary",
            "business_unit",
            "business_units",
            "date_field",
            "delete",
            "distinct",
            "measure_units",
            "range",
            "reduce",
            "references",
            "right",
            "set",
            "union",
            "any",
            "describe",
            "exists",
            "in",
            "rule",
            "rules",
            "custom",
            "import",
            "export",
            "exp",
            "business_categories",
            "business_category"
        );
        StringBuilder stringBuilder = new StringBuilder("create dim table a(\n");
        int size = 0;
        for (String column : nonReserved) {
            stringBuilder.append(String.format("%s bigint ", column));
            size++;
            if (size < nonReserved.size()) {
                stringBuilder.append(",");
            }
        }
        stringBuilder.append(")");
        CreateDimTable parse = parse(stringBuilder.toString(), CreateDimTable.class);
        assertNotNull(parse);
    }

    @Test
    public void testReverseError() {

        String fml = "CREATE DIM TABLE IF NOT EXISTS dim_hm_put_spmabc \n"
            + "(\n"
            + "   spm_ab string COMMENT '页面AB' WITH ('dict'='spm_ab'),\n"
            + "   spm_abc string COMMENT '页面ABC' WITH ('dict'='spm_abc'),\n"
            + "   spm_name string COMMENT 'spm名称' WITH ('dict'='spm_name'),\n"
            + "   etl_time string COMMENT '数据处理时间' WITH ('dict'='etl_time')\n"
            + ")\n"
            + " COMMENT '投放页面-spmabc信息表-用于blink实时计算'\n"
            + " PARTITIONED BY (ds string COMMENT '账期字段：YYYYMMDD')";

        CreateDimTable parse = parse(fml, CreateDimTable.class);
        assertNotNull(parse);
    }
}
