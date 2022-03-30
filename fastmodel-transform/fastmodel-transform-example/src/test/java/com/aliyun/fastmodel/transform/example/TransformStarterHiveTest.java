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

import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/3/19
 */
public class TransformStarterHiveTest {

    TransformStarter example = new TransformStarter();

    @Test
    public void testTx() {
        String before = "CREATE FACT TABLE emr_pre_simple.sunyupengtest5 (\n"
            + "   sunyupengtest1 STRING CORRELATION COMMENT 'sunyupengtest1' WITH "
            + "('uuid'='tb_c-103db3c08dfb4834bf61b67de9d337be')\n"
            + ")\n"
            + " COMMENT 'sunyupengtest5'\n"
            + " WITH ('type'='tx')";

        String after =
            " CREATE FACT TABLE emr_pre_simple.sunyupengtest5 (\n"
                + "   sunyupengtest1 STRING CORRELATION COMMENT 'sunyupengtest1' WITH "
                + "('uuid'='tb_c-103db3c08dfb4834bf61b67de9d337be'),\n"
                + "   sunyupengtest2 STRING CORRELATION COMMENT 'sunyupengtest2' WITH "
                + "('uuid'='tb_c-8663a50e035a4d2c97764cb7e6de6527')\n"
                + ")\n"
                + " COMMENT 'sunyupengtest5'\n"
                + " WITH ('type'='tx')";

        String s = example.compareAndTransformFml(before, after, DialectMeta.getHive(),
            TransformContext.builder().build());
        assertEquals(s, "ALTER TABLE sunyupengtest5 ADD COLUMNS\n"
            + "(\n"
            + "   sunyupengtest2 STRING COMMENT 'sunyupengtest2'\n"
            + ")");
    }
}
