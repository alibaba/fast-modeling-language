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

package com.aliyun.fastmodel.transform.api.util;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import org.junit.Assert;
import org.junit.Test;

public class StringJoinUtilTest {

    @Test
    public void join_NullInput_ReturnsNull() {
        QualifiedName result = StringJoinUtil.join((String[])null);
        Assert.assertNull(result);
    }

    @Test
    public void join_EmptyArray_ReturnsNull() {
        QualifiedName result = StringJoinUtil.join();
        Assert.assertNull(result);
    }

    @Test
    public void join_LastArgumentIsBlank_ReturnsQualifiedNameWithSingleIdentifier() {
        QualifiedName result = StringJoinUtil.join("a", "b", " ");
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.getOriginalParts().size());
        Assert.assertEquals(" ", result.getOriginalParts().get(0).getValue());
    }

    @Test
    public void join_AllArgumentsAreBlank_ReturnsQualifiedNameWithSingleIdentifier() {
        QualifiedName result = StringJoinUtil.join(" ", " ", " ");
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.getOriginalParts().size());
    }

    @Test
    public void join_MixedArguments_ReturnsQualifiedNameWithNonBlankIdentifiers() {
        QualifiedName result = StringJoinUtil.join("a", " ", "b", "c");
        Assert.assertNotNull(result);
        List<Identifier> expectedIdentifiers = new ArrayList<>();
        expectedIdentifiers.add(new Identifier("a"));
        expectedIdentifiers.add(new Identifier("b"));
        expectedIdentifiers.add(new Identifier("c"));
        Assert.assertEquals(expectedIdentifiers, result.getOriginalParts());
    }

    @Test
    public void join_AllArgumentsAreNonBlank_ReturnsQualifiedNameWithAllIdentifiers() {
        QualifiedName result = StringJoinUtil.join("a", "b", "c");
        Assert.assertNotNull(result);
        List<Identifier> expectedIdentifiers = new ArrayList<>();
        expectedIdentifiers.add(new Identifier("a"));
        expectedIdentifiers.add(new Identifier("b"));
        expectedIdentifiers.add(new Identifier("c"));
        Assert.assertEquals(expectedIdentifiers, result.getOriginalParts());
    }
}
