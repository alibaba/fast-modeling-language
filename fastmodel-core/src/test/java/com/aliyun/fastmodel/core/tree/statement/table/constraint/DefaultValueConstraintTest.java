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

package com.aliyun.fastmodel.core.tree.statement.table.constraint;

import java.util.List;

import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * DefaultValueConstraintTest
 *
 * @author panguanjing
 * @date 2021/8/30
 */
public class DefaultValueConstraintTest {

    List<BaseConstraint> list = null;

    @Before
    public void setUp() throws Exception {
        list = ImmutableList.of(new DefaultValueConstraint(IdentifierUtil.sysIdentifier(), new StringLiteral("a")));
    }

    @Test
    public void testEquals() {
        DefaultValueConstraint defaultValueConstraint = new DefaultValueConstraint(IdentifierUtil.sysIdentifier(),
            new StringLiteral("a"));
        assertTrue(list.contains(defaultValueConstraint));
    }
}