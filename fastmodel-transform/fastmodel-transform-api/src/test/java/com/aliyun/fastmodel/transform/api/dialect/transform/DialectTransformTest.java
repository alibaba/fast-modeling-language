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

package com.aliyun.fastmodel.transform.api.dialect.transform;

import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.dialect.transform.DialectTransformParam.DialectTransformParamBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/8/14
 */
public class DialectTransformTest {

    @Test(expected = NullPointerException.class)
    public void transform() {
        DialectTransformParamBuilder builder = DialectTransformParam.builder();
        builder.sourceMeta(DialectMeta.DEFAULT_MYSQL);
        builder.targetMeta(DialectMeta.DEFAULT_HIVE);
        DialectNode transform = DialectTransform.transform(builder.build());
        assertNull(transform);
    }

    @Test
    public void transformEqual() {
        DialectTransformParamBuilder builder = DialectTransformParam.builder();
        builder.sourceMeta(DialectMeta.DEFAULT_MYSQL);
        builder.targetMeta(DialectMeta.DEFAULT_MYSQL);
        DialectNode sourceNode = new DialectNode("select * from abc");
        builder.sourceNode(sourceNode);
        DialectNode transform = DialectTransform.transform(builder.build());
        assertEquals(transform, sourceNode);
    }
}