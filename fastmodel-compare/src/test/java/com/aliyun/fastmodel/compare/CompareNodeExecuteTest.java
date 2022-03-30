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

package com.aliyun.fastmodel.compare;

import java.util.List;

import com.aliyun.fastmodel.core.formatter.FastModelFormatter;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.businessprocess.CreateBusinessProcess;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDwsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateFactTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/2/5
 */
public class CompareNodeExecuteTest {

    CompareNodeExecute compareNodeExecute = CompareNodeExecute.getInstance();

    @Test
    public void compare() {
        CreateDimTable createDimTable = CreateDimTable.builder().tableName(
            QualifiedName.of("a.b")
        ).build();
        CreateDimTable createDimTable1 = CreateDimTable.builder().tableName(
            QualifiedName.of("a.c")
        ).build();
        List<BaseStatement> statements = compareNodeExecute.compare(createDimTable, createDimTable1,
            CompareStrategy.INCREMENTAL);
        assertEquals(1, statements.size());
        BaseStatement statement = statements.get(0);
        assertEquals(getExpected(statement), "ALTER TABLE a.b RENAME TO a.c");
    }

    @Test
    public void compareDimFact() {
        CreateDimTable createDimTable = CreateDimTable.builder().tableName(
            QualifiedName.of("a.b")
        ).build();
        CreateFactTable createDimTable1 = CreateFactTable.builder().tableName(
            QualifiedName.of("a.c")
        ).build();
        List<BaseStatement> statements = compareNodeExecute.compare(createDimTable, createDimTable1,
            CompareStrategy.INCREMENTAL);
        assertEquals(1, statements.size());
        BaseStatement statement = statements.get(0);
        assertEquals(getExpected(statement), "ALTER TABLE a.b RENAME TO a.c");
    }

    @Test
    public void compareWithDiffTableType() {
        CreateTable createDimTable = CreateTable.builder().tableName(
            QualifiedName.of("a.b")
        ).build();
        CreateDimTable createDimTable1 = CreateDimTable.builder().tableName(
            QualifiedName.of("a.c")
        ).build();
        List<BaseStatement> statements = compareNodeExecute.compare(createDimTable, createDimTable1,
            CompareStrategy.INCREMENTAL);
        assertEquals(1, statements.size());
        BaseStatement statement = statements.get(0);
        assertEquals(getExpected(statement), "ALTER TABLE a.b RENAME TO a.c");
    }

    @Test
    public void compareWithDws() {
        CreateDwsTable createDimTable = CreateDwsTable.builder().tableName(
            QualifiedName.of("a.b")
        ).build();
        CreateTable createDimTable1 = CreateTable.builder().tableName(
            QualifiedName.of("a.c")
        ).build();
        List<BaseStatement> statements = compareNodeExecute.compare(createDimTable, createDimTable1,
            CompareStrategy.INCREMENTAL);
        assertEquals(1, statements.size());
        BaseStatement statement = statements.get(0);
        assertEquals(getExpected(statement), "ALTER TABLE a.b RENAME TO a.c");
    }

    private String getExpected(BaseStatement statement) {
        return FastModelFormatter.formatNode(statement);
    }

    @Test(expected = ClassCastException.class)
    public void testException() {
        compareNodeExecute.compare(
            CreateDimTable.builder().tableName(QualifiedName.of("a.b")).build(),
            new CreateBusinessProcess(CreateElement.builder().qualifiedName(QualifiedName.of("b.c")).build()),
            CompareStrategy.INCREMENTAL);
    }

    @Test
    public void testNull() {
        List<BaseStatement> compare = compareNodeExecute.compare(null, null, CompareStrategy.INCREMENTAL);
        assertEquals(compare.size(), 0);
    }

    @Test
    public void testBeforeNullAndAfterNotNull() {
        List<BaseStatement> statements = compareNodeExecute.compare(null,
            CreateDimTable.builder().tableName(QualifiedName.of("a.b")).build(), CompareStrategy.INCREMENTAL);
        assertEquals(1, statements.size());

        BaseStatement statement = statements.get(0);
        assertEquals(getExpected(statement), "CREATE DIM TABLE a.b");
    }

    @Test
    public void testBeforeNotNullAndAfterIsNull() {
        List<BaseStatement> compare = compareNodeExecute.compare(CreateDimTable.builder().tableName(
            QualifiedName.of("a.b")
        ).build(), null, CompareStrategy.INCREMENTAL);
        BaseStatement statement = compare.get(0);
        assertEquals(getExpected(statement), "DROP TABLE IF EXISTS a.b");
    }

    @Test
    public void testComposite() {
        CreateDwsTable createDimTable = CreateDwsTable.builder().tableName(
            QualifiedName.of("a.b")
        ).build();
        CreateTable createTable = CreateTable.builder().tableName(
            QualifiedName.of("a.c")
        ).build();
        CompositeStatement before = new CompositeStatement(
            Lists.newArrayList(createDimTable)
        );
        CompositeStatement after = new CompositeStatement(
            Lists.newArrayList(createTable)
        );
        List<BaseStatement> compare = compareNodeExecute.compare(before, after, CompareStrategy.INCREMENTAL);
        assertEquals(1, compare.size());
        BaseStatement baseStatement = compare.get(0);
        Assert.assertEquals(baseStatement.toString(), "ALTER TABLE a.b RENAME TO a.c");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testNotSupport() {
        CreateBusinessProcess createBusinessProcess = new CreateBusinessProcess(
            CreateElement.builder()
                .qualifiedName(QualifiedName.of("business_process"))
                .build()
        );
        CreateBusinessProcess createBusinessProcess2 = new CreateBusinessProcess(
            CreateElement.builder()
                .qualifiedName(QualifiedName.of("business_process_2"))
                .build()
        );
        List<BaseStatement> baseStatementList = compareNodeExecute.compareNode(createBusinessProcess,
            createBusinessProcess2, CompareStrategy.INCREMENTAL);

    }

    @Test
    public void testCompareFull() {
        BaseStatement before = CreateDwsTable.builder().tableName(
            QualifiedName.of("a.b")
        ).build();
        ;

        CreateTable createTable = CreateTable.builder().tableName(
            QualifiedName.of("a.c")
        ).build();
        List<BaseStatement> compare = compareNodeExecute.compare(before, createTable, CompareStrategy.FULL);
        assertEquals("DROP TABLE IF EXISTS a.b", compare.get(0).toString());
    }
}