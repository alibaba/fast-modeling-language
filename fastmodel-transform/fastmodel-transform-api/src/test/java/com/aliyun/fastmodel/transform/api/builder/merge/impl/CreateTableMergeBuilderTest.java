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

package com.aliyun.fastmodel.transform.api.builder.merge.impl;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.builder.merge.exception.MergeException;
import com.google.common.collect.Lists;
import org.junit.Test;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/5/1
 */
public class CreateTableMergeBuilderTest {

    @Test(expected = MergeException.class)
    public void testGetMainStatement() {
        CreateTableMergeBuilder createTableMergeBuilder = new CreateTableMergeBuilder();
        List<BaseStatement> baseStatements = Lists.newArrayList(
            CreateTable.builder().tableName(QualifiedName.of("t1")).build(),
            CreateTable.builder().tableName(QualifiedName.of("t2")).build()
        );
        createTableMergeBuilder.getMainStatement(baseStatements);
    }

    @Test(expected = MergeException.class)
    public void testGetMainStatement2() {
        CreateTableMergeBuilder createTableMergeBuilder = new CreateTableMergeBuilder();
        List<BaseStatement> baseStatements = Lists.newArrayList(
        );
        createTableMergeBuilder.getMainStatement(baseStatements);
    }
}