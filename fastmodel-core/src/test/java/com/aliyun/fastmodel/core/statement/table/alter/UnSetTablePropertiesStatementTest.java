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

package com.aliyun.fastmodel.core.statement.table.alter;

import java.util.Arrays;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.aliyun.fastmodel.core.tree.statement.table.UnSetTableProperties;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/10/15
 */
public class UnSetTablePropertiesStatementTest {

    @Test
    public void testToString() {
        UnSetTableProperties unSetPropertiesStatement = new UnSetTableProperties(QualifiedName.of("b", "d"),
            Arrays.asList("123", "134"));
        unSetPropertiesStatement.setStatementType(StatementType.TABLE);
        assertNotNull(unSetPropertiesStatement.toString());
    }
}