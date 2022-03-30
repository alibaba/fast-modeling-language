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

package com.aliyun.fastmodel.driver.cli.command;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/1/19
 */
public class HelpTest {

    @Test
    public void testHelp() {

        assertEquals("Supported commands:\n"
            + "HELP\n"
            + "QUIT\n"
            + "-------- SELECT & SHOW ----\n"
            + "DESCRIBE TABLE <table>\n"
            + "DESCRIBE INDICATOR <indicator>\n"
            + "SHOW CREATE TABLE <table>\n"
            + "SHOW CREATE INDICATOR <indicator>\n"
            + "SHOW TABLES | LAYERS | DOMAINS  [FROM <schema>] [LIKE <pattern>]\n"
            + "SHOW ATOMIC|DERIVATIVE INDICATORS [FROM <schema>] [LIKE <pattern>]\n"
            + "SHOW TIMEPERIODS [LIKE <pattern>]\n"
            + "SHOW ADJUNCT [LIKE <pattern>]\n"
            + "SHOW LAYERS\n"
            + "------DDL------\n"
            + "CREATE DIM|FACT TABLE\n"
            + "CREATE ATOMIC | DERIVATIVE INDICATOR\n"
            + "CREATE BUSINESSPROCESS\n"
            + "CREATE DOMAIN\n"
            + "CREATE DICT\n"
            + "CREATE MEASUREUNIT\n"
            + "CREATE ADJUNCT\n"
            + "CREATE TIMEPERIOD\n"
            + "CREATE LAYER\n"
            + "ALTER TABLE\n"
            + "ALTER INDICATOR\n"
            + "ALTER DOMAIN\n"
            + "ALTER BUSINESSPROCESS\n"
            + "ALTER DICT\n"
            + "ALTER MEASUREUNIT\n"
            + "ALTER ADJUNCT\n"
            + "ALTER TIMEPERIOD\n"
            + "DROP TABLE\n"
            + "DROP INDICATOR\n", Help.getHelpText());
    }
}