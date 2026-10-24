/*
 * Copyright 2021-2023 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.example.dialect;

import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.dialect.transform.DialectTransform;
import com.aliyun.fastmodel.transform.api.dialect.transform.DialectTransformParam;
import com.aliyun.fastmodel.transform.example.BaseTransformCaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * SQLite to MaxCompute type conversion
 *
 * @author panguanjing
 * @date 2022/8/13
 */
public class Sqlite2MaxComputeTest extends BaseTransformCaseTest {

    @Test
    public void testSqliteToMaxCompute() {
        String maxComputeDDL = "create table m1 (m text, v int, c varchar(10));";
        DialectTransformParam param = DialectTransformParam.builder()
            .sourceMeta(DialectMeta.getByName(DialectName.SQLITE))
            .targetMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .sourceNode(new DialectNode(maxComputeDDL))
            .build();
        DialectNode transform = DialectTransform.transform(param);
        String node = transform.getNode();
        assertEquals(node, "CREATE TABLE m1\n"
            + "(\n"
            + "   m STRING,\n"
            + "   v INT,\n"
            + "   c STRING\n"
            + ")\n");
    }

}
