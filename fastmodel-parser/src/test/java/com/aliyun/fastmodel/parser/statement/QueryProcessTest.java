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

package com.aliyun.fastmodel.parser.statement;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.parser.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/1/20
 */
public class QueryProcessTest extends BaseTest {

    @Test
    public void testProcess() {
        String sql = "SELECT  rel.pool_id\n"
            + "        ,rel.relation_id\n"
            + "        ,prize.*\n"
            + "FROM    (\n"
            + "            SELECT  *\n"
            + "                    ,'dd' AS HOUR\n"
            + "            FROM    antods.ods_kn_prize_pool_relation\n"
            + "            WHERE   dt = '20210117'\n"
            + "            AND     pool_id IN ('PP202006131310','PP202006121286')\n"
            + "            UNION\n"
            + "            SELECT  *\n"
            + "            FROM    antods.ods_kn_prize_pool_relation_delta_hh\n"
            + "            WHERE   dt = '20210118'\n"
            + "            AND     HOUR = '23'\n"
            + "            AND     pool_id IN ('PP202006131310','PP202006121286')\n"
            + "        ) AS rel\n"
            + "LEFT JOIN (\n"
            + "              SELECT  *\n"
            + "                      ,'dd' AS `HOUR`\n"
            + "              FROM    antods.ods_kn_prize\n"
            + "              WHERE   dt = '20210117'\n"
            + "              UNION\n"
            + "              SELECT  *\n"
            + "              FROM    antods.ods_kn_prize_delta_hh\n"
            + "              WHERE   dt = '20210118'\n"
            + "              AND     HOUR = '23'\n"
            + "          ) AS prize\n"
            + "ON      prize.prize_id = rel.prize_id\n"
            + "WHERE   prize_type != \"VOUCHER_PRIZE\"";
        BaseStatement statement = nodeParser.parseStatement(sql);
        assertEquals(statement.toString(), "SELECT\n"
            + "  rel.pool_id\n"
            + ", rel.relation_id\n"
            + ", prize.*\n"
            + "FROM\n"
            + "  (\n"
            + "      SELECT\n"
            + "        *\n"
            + "      , 'dd' hour\n"
            + "      FROM\n"
            + "        antods.ods_kn_prize_pool_relation\n"
            + "      WHERE dt = '20210117' AND pool_id IN ('PP202006131310', 'PP202006121286')\n"
            + "UNION       SELECT *\n"
            + "      FROM\n"
            + "        antods.ods_kn_prize_pool_relation_delta_hh\n"
            + "      WHERE dt = '20210118' AND hour = '23' AND pool_id IN ('PP202006131310', 'PP202006121286')\n"
            + "   )  rel\n"
            + "LEFT JOIN (\n"
            + "      SELECT\n"
            + "        *\n"
            + "      , 'dd' `hour`\n"
            + "      FROM\n"
            + "        antods.ods_kn_prize\n"
            + "      WHERE dt = '20210117'\n"
            + "UNION       SELECT *\n"
            + "      FROM\n"
            + "        antods.ods_kn_prize_delta_hh\n"
            + "      WHERE dt = '20210118' AND hour = '23'\n"
            + "   )  prize ON prize.prize_id = rel.prize_id\n"
            + "WHERE prize_type != 'VOUCHER_PRIZE'\n");
    }
}
