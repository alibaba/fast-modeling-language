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

package com.aliyun.fastmodel.transform.example;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.hive.context.HiveTransformContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/2/24
 */
public class TransformTableTest extends BaseTransformCaseTest {

    @Test
    public void testSetTableComment_Hive() {
        BaseStatement b
            = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).build();
        BaseStatement a = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).comment(new Comment("abc"))
            .build();
        String text = starter.compareAndTransform(b, a, DialectMeta.getHive(), HiveTransformContext.builder().build());
        assertEquals("ALTER TABLE b SET TBLPROPERTIES('comment'='abc')", text);
    }
}
