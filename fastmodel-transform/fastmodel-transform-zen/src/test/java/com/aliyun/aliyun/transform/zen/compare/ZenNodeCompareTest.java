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

package com.aliyun.aliyun.transform.zen.compare;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.transform.api.compare.CompareContext;
import com.aliyun.fastmodel.transform.api.compare.CompareResult;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * zenNodecompare test
 *
 * @author panguanjing
 * @date 2021/9/17
 */
public class ZenNodeCompareTest {
    ZenNodeCompare zenNodeCompare = new ZenNodeCompare();

    @Test
    public void testCompare() {
        DialectNode before = new DialectNode("user_id\nuser_name");
        DialectNode after = new DialectNode("user_id\nuser_name\nuser_age");
        CompareResult compareResult = zenNodeCompare.compareResult(before, after,
            CompareContext.builder().qualifiedName(
                    QualifiedName.of("dim_shop"))
                .build());
        BaseStatement beforeStatement = compareResult.getBeforeStatement();
        BaseStatement afterStatement = compareResult.getAfterStatement();
        assertEquals(beforeStatement.toString(), "CREATE TABLE dim_shop \n"
            + "(\n"
            + "   user_id   STRING COMMENT 'user_id',\n"
            + "   user_name STRING COMMENT 'user_name'\n"
            + ")");
        assertEquals(afterStatement.toString(), "CREATE TABLE dim_shop \n"
            + "(\n"
            + "   user_id   STRING COMMENT 'user_id',\n"
            + "   user_name STRING COMMENT 'user_name',\n"
            + "   user_age  STRING COMMENT 'user_age'\n"
            + ")");
        assertEquals(compareResult.getDiffStatements().toString(),
            "[ALTER TABLE dim_shop ADD COLUMNS\n"
                + "(\n"
                + "   user_age STRING COMMENT 'user_age'\n"
                + ")]");
    }

    @Test
    public void testCompareBeforeNull() {
        DialectNode before = new DialectNode(null);
        DialectNode after = new DialectNode("user_id\nuser+name");
        CompareResult compareResult = zenNodeCompare.compareResult(before, after,
            CompareContext.builder().qualifiedName(QualifiedName.of("dim_sho")).build());
        assertEquals(compareResult.getDiffStatements().size(), 1);
    }

    @Test
    public void testCompareAfterNull() {
        DialectNode after = new DialectNode(null);
        DialectNode before = new DialectNode("user_id\nuser+name");
        List<BaseStatement> diff = zenNodeCompare.compare(before, after,
            CompareContext.builder().qualifiedName(QualifiedName.of("dim_sho")).build());
        assertEquals(diff.size(), 3);

    }
}