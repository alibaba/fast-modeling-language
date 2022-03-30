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

package com.aliyun.fastmodel.core.tree;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author panguanjing
 * @date 2020/11/18
 */
public class QualifiedNameTest {

    @Test
    public void testOf() {
        String table = "junit.table";
        QualifiedName of = QualifiedName.of(table);
        assertEquals(of.getParts().get(0), "junit");
        assertEquals(of.getParts().get(1), "table");
    }

    @Test
    public void testgetFirstIfSizeOverOne() {
        QualifiedName a = QualifiedName.of("a");
        String firstIfSizeOverOne = a.getFirstIfSizeOverOne();
        assertEquals(null, firstIfSizeOverOne);
    }

    @Test
    public void testGetSuffix() {
        QualifiedName of = QualifiedName.of("a.b");
        assertEquals(of.getFirst(), "a");
        assertEquals(of.getSuffix(), "b");
        assertEquals(of.getSuffixPath(), "b");
    }

    @Test
    public void testGetSuffixPath() {
        QualifiedName of = QualifiedName.of("a");
        assertEquals(of.getSuffixPath(), "a");
        of = QualifiedName.of("a.b.c.e.f");
        assertEquals(of.getSuffixPath(), "b.c.e.f");
        assertEquals(of.getSuffix(), "f");
    }

    @Test
    public void testPrefixPath() {
        QualifiedName of = QualifiedName.of("a.b.c");
        String prefixPath = of.getPrefixPath();
        assertEquals(prefixPath, "a.b");
        of = QualifiedName.of("c");
        prefixPath = of.getPrefixPath();
        assertNull(prefixPath);
        of = QualifiedName.of("b.c");
        prefixPath = of.getPrefixPath();
        assertEquals(prefixPath, "b");
    }
}