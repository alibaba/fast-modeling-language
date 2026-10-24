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
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.dialect.transform.DialectTransform;
import com.aliyun.fastmodel.transform.api.dialect.transform.DialectTransformParam;
import com.aliyun.fastmodel.transform.example.BaseTransformCaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * maxcompute to hologres converter
 *
 * @author panguanjing
 * @date 2022/8/13
 */
public class MaxComputeToHologresTest extends BaseTransformCaseTest {

    @Test
    public void testMaxComputeToHologres() {
        String maxComputeDDL = "create table m1 (m bigint) comment 'abc';";
        DialectTransformParam param = DialectTransformParam.builder()
            .sourceMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .targetMeta(DialectMeta.DEFAULT_HOLO)
            .sourceNode(new DialectNode(maxComputeDDL))
            .build();
        DialectNode transform = DialectTransform.transform(param);
        String node = transform.getNode();
        assertEquals(node, "BEGIN;\n"
            + "CREATE TABLE m1 (\n"
            + "   m BIGINT\n"
            + ");\n"
            + "COMMENT ON TABLE m1 IS 'abc';\n"
            + "COMMIT;");
    }

    @Test
    public void testHologresToMaxCompute() {
        String hologresDdl = "create table m1 (m text);";
        DialectTransformParam param = DialectTransformParam.builder()
            .sourceMeta(DialectMeta.DEFAULT_HOLO)
            .targetMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .sourceNode(new DialectNode(hologresDdl))
            .build();
        DialectNode transform = DialectTransform.transform(param);
        String node = transform.getNode();
        assertEquals(node, "CREATE TABLE m1\n"
            + "(\n"
            + "   m STRING\n"
            + ")\n");
    }

    @Test
    public void testHologresToMaxCompute2() {
        String hologresDdl = "create table m1 (m int4[]);";
        DialectTransformParam param = DialectTransformParam.builder()
            .sourceMeta(DialectMeta.DEFAULT_HOLO)
            .targetMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .sourceNode(new DialectNode(hologresDdl))
            .build();
        DialectNode transform = DialectTransform.transform(param);
        String node = transform.getNode();
        assertEquals(node, "CREATE TABLE m1\n"
            + "(\n"
            + "   m ARRAY<INT>\n"
            + ")\n");
    }
}
