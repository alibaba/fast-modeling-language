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

package com.aliyun.fastmodel.transform.example.integrate;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.TransformerFactory;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import org.apache.commons.io.IOUtils;

/**
 * 转换内容
 *
 * @author panguanjing
 * @date 2021/7/8
 */
public class MysqlTransformIntegrate extends AbstractTransformIntegrate {
    @Override
    public String getExpect() {
        try {
            return IOUtils.resourceToString("/example/mysql.ddl.txt", StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("getExpect occur exception", e);
        }
    }

    @Override
    public Transformer getTransformer() {
        return TransformerFactory.getInstance().get(DialectMeta.DEFAULT_MYSQL);
    }
}
