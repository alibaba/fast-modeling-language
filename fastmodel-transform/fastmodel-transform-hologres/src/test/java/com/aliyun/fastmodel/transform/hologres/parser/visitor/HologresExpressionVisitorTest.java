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

package com.aliyun.fastmodel.transform.hologres.parser.visitor;

import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.transform.hologres.context.HologresTransformContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/3/26
 */
public class HologresExpressionVisitorTest {

    @Test
    public void testNumberIdentifier() {
        HologresExpressionVisitor hologresExpressionVisitor = new HologresExpressionVisitor(HologresTransformContext.builder().build());
        String t = "111_schema";
        String value = hologresExpressionVisitor.visitIdentifier(new Identifier(t), null);
        assertEquals("\"111_schema\"", value);
    }

    @Test
    public void testKeyword() {
        HologresExpressionVisitor hologresExpressionVisitor = new HologresExpressionVisitor(HologresTransformContext.builder().build());
        String t = "schema";
        String value = hologresExpressionVisitor.visitIdentifier(new Identifier(t), null);
        assertEquals("\"schema\"", value);
    }
}