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

package com.aliyun.aliyun.transform.zen;

import com.aliyun.aliyun.transform.zen.parser.ZenParserImpl;
import com.aliyun.aliyun.transform.zen.parser.tree.AtomicZenExpression;
import com.aliyun.aliyun.transform.zen.parser.tree.BaseZenExpression;
import com.aliyun.aliyun.transform.zen.parser.tree.BaseZenNode;
import com.aliyun.aliyun.transform.zen.parser.tree.BrotherZenExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * ZenParserImplTest
 *
 * @author panguanjing
 * @date 2021/7/15
 */
public class ZenParserImplTest {

    ZenParserImpl zenParser = new ZenParserImpl();

    @Test
    public void parseZenCode() {
        String zenCode = "user_id\nuser_name";
        BrotherZenExpression baseZenNode = (BrotherZenExpression)zenParser.parseNode(zenCode);
        BaseZenExpression left = baseZenNode.getLeft();
        BaseZenExpression right = baseZenNode.getRight();
        assertNotNull(left);
        AtomicZenExpression atomicZenExpression = (AtomicZenExpression)left;
        Identifier identifier = atomicZenExpression.getIdentifier();
        assertEquals(identifier, new Identifier("user_id"));
    }

    @Test
    public void testCtrl() {
        String zenCode = "user_id\r\nuser_name\n";
        BrotherZenExpression brotherZenExpression = (BrotherZenExpression)zenParser.parseNode(zenCode);
        assertNotNull(brotherZenExpression);
    }

    @Test
    public void testAttribute() {
        String zenCode = "user_id|user_name|user_age.bigint";
        BaseZenNode baseZenNode = zenParser.parseNode(zenCode);
        assertEquals(baseZenNode.getClass(), BrotherZenExpression.class);
    }
}