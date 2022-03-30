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

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.constants.ShowType;
import com.aliyun.fastmodel.core.tree.statement.references.MoveReferences;
import com.aliyun.fastmodel.core.tree.statement.references.ShowReferences;
import com.aliyun.fastmodel.parser.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * References Test
 *
 * @author panguanjing
 * @date 2022/2/18
 */
public class ReferencesTest extends BaseTest {

    @Test
    public void testMove() {
        String fml = "move domain references from a to b;";
        MoveReferences parse = parse(fml, MoveReferences.class);
        assertEquals(parse.getShowType(), ShowType.DOMAIN);
        assertEquals(parse.getFrom(), QualifiedName.of("a"));
        assertEquals(parse.getTo(), QualifiedName.of("b"));
    }

    @Test
    public void testShowDependency() {
        String fml = "show domain REFERENCES from tmall";
        ShowReferences parse = parse(fml, ShowReferences.class);
        assertEquals(parse.getQualifiedName(), QualifiedName.of("tmall"));
    }

    @Test
    public void testShowDependencyWithProperties() {
        String fml = "show domain REFERENCES from tmall with ('p' = 'b')";
        ShowReferences parse = parse(fml, ShowReferences.class);
        assertEquals(parse.getProperties().size(), 1);
    }
}
