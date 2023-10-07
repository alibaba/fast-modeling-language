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

package com.aliyun.fastmodel.core.tree.statement;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author panguanjing
 * @date 2020/11/17
 */
public class BaseOperatorStatementTest {

    @Test
    public void testGetIdentifier() {
        BaseOperatorStatement baseOperatorStatement = new DropTable(QualifiedName.of("a", "b"));
        assertEquals(baseOperatorStatement.getBusinessUnit(), "a");
        assertEquals(baseOperatorStatement.getIdentifier(), "b");
    }

    @Test
    public void testGetUnit() {
        BaseOperatorStatement baseOperatorStatement = new DropTable(QualifiedName.of("a"));
        assertNull(baseOperatorStatement.getBusinessUnit());
        assertEquals(baseOperatorStatement.getIdentifier(), "a");
    }

    @Test
    public void testSetBusinessUnit() {
        BaseOperatorStatement baseOperatorStatement = new DropTable(QualifiedName.of("a", "b"));
        baseOperatorStatement.setBusinessUnit("c");
        assertEquals(baseOperatorStatement.getBusinessUnit(), "c");
        assertEquals(baseOperatorStatement.getIdentifier(), "b");
    }

    @Test
    public void testSetBusinessUnit2() {
        BaseOperatorStatement baseOperatorStatement = new DropTable(QualifiedName.of("b"));
        baseOperatorStatement.setBusinessUnit("c");
        assertEquals(baseOperatorStatement.getBusinessUnit(), "c");
        assertEquals(baseOperatorStatement.getIdentifier(), "b");
    }

}