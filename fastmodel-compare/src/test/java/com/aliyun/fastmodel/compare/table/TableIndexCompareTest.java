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

package com.aliyun.fastmodel.compare.table;

import java.util.List;

import com.aliyun.fastmodel.compare.impl.table.TableIndexCompare;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.index.IndexColumnName;
import com.aliyun.fastmodel.core.tree.statement.table.index.TableIndex;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * TableIndexCompareTest
 *
 * @author panguanjing
 * @date 2021/8/31
 */
public class TableIndexCompareTest {

    TableIndexCompare tableIndexCompare = new TableIndexCompare();

    @Test
    public void testTableIndex() {
        CreateTable before =
            CreateTable.builder().tableName(QualifiedName.of("abc")).build();
        CreateTable after =
            CreateTable.builder().tableName(QualifiedName.of("abc")).tableIndex(
                ImmutableList.of(
                    new TableIndex(
                        new Identifier("index_name"),
                        ImmutableList.of(new IndexColumnName(new Identifier("a"), null, null)),
                        null
                    ))
            ).build();
        List<BaseStatement> baseStatementList = tableIndexCompare.compareTableElement(before, after);
        assertEquals(1, baseStatementList.size());
        assertEquals(baseStatementList.get(0).toString(), "CREATE INDEX index_name ON abc (a)");
    }

    @Test
    public void testTableIndexDrop() {
        TableIndex element = new TableIndex(
            new Identifier("index_name"),
            ImmutableList.of(new IndexColumnName(new Identifier("a"), null, null)),
            null
        );
        CreateTable before =
            CreateTable.builder().tableName(QualifiedName.of("abc")).tableIndex(Lists.newArrayList(element)).build();
        CreateTable after =
            CreateTable.builder().tableName(QualifiedName.of("abc")).build();
        List<BaseStatement> baseStatementList = tableIndexCompare.compareTableElement(before, after);
        assertEquals(1, baseStatementList.size());
        assertEquals(baseStatementList.get(0).toString(), "DROP INDEX index_name ON abc");
    }

    @Test
    public void testTableIndexAddDrop() {
        TableIndex element = new TableIndex(
            new Identifier("index_name"),
            ImmutableList.of(new IndexColumnName(new Identifier("a"), null, null)),
            null
        );
        TableIndex element2 = new TableIndex(
            new Identifier("index_name"),
            ImmutableList.of(new IndexColumnName(new Identifier("b"), null, null)),
            null
        );
        CreateTable before =
            CreateTable.builder().tableName(QualifiedName.of("abc")).tableIndex(Lists.newArrayList(element)).build();
        CreateTable after =
            CreateTable.builder().tableName(QualifiedName.of("abc")).tableIndex(Lists.newArrayList(element2)).build();
        List<BaseStatement> baseStatementList = tableIndexCompare.compareTableElement(before, after);
        assertEquals(2, baseStatementList.size());
        assertEquals(baseStatementList.get(0).toString(), "DROP INDEX index_name ON abc");
        assertEquals(baseStatementList.get(1).toString(), "CREATE INDEX index_name ON abc (b)");
    }
}