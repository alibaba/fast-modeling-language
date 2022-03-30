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

package com.aliyun.fastmodel.transform.fml.compare;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Fml Node Compare
 *
 * @author panguanjing
 * @date 2021/9/4
 */
public class FmlNodeCompareTest {

    private FmlNodeCompare fmlNodeCompare = new FmlNodeCompare();

    @Test
    public void testCompare() {
        DialectNode before = new DialectNode("create dim table a (b bigint)");
        DialectNode after = new DialectNode("create dim table b (b bigint comment 'abc')");
        List<BaseStatement> compare = fmlNodeCompare.compare(before, after);
        String collect = compare.stream().map(BaseStatement::toString).collect(Collectors.joining("\n"));
        assertEquals(collect, "ALTER TABLE a RENAME TO b\n"
            + "ALTER TABLE b CHANGE COLUMN b b BIGINT COMMENT 'abc'");
    }

    @Test
    public void testCompare_with_delimited() {
        DialectNode before = new DialectNode("create dim table a (`b` bigint)");
        DialectNode after = new DialectNode("create dim table a (b bigint)");
        List<BaseStatement> compare = fmlNodeCompare.compare(before, after);
        String collect = compare.stream().map(BaseStatement::toString).collect(Collectors.joining("\n"));
        assertEquals(collect, "");

    }
}