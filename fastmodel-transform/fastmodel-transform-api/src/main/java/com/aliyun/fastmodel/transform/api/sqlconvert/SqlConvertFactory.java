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

package com.aliyun.fastmodel.transform.api.sqlconvert;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.dialect.Dialect;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;

/**
 * 转换器工厂
 *
 * @author panguanjing
 * @date 2020/10/16
 */
public class SqlConvertFactory {

    private static final SqlConvertFactory INSTANCE = new SqlConvertFactory();

    private final Map<String, SqlConverter> maps = new HashMap<>();

    /**
     * 私有构造函数，只在内容进行调用
     */
    private SqlConvertFactory() {
        ServiceLoader<SqlConverter> load = ServiceLoader.load(SqlConverter.class);
        for (SqlConverter transformer : load) {
            Dialect annotation = transformer.getClass().getAnnotation(Dialect.class);
            if (annotation != null) {
                maps.put(annotation.value() + annotation.version(), transformer);
            }
        }
    }

    public static SqlConvertFactory getInstance() {
        return INSTANCE;
    }

    /**
     * 根据engineMeta获取
     *
     * @param dialectMeta {@link DialectMeta}
     * @return {@link Transformer}
     */
    public SqlConverter get(DialectMeta dialectMeta) {
        if (dialectMeta == null) {
            throw new IllegalArgumentException("dialectMeta can't be null");
        }
        SqlConverter statementTransformer = maps.get(dialectMeta.toString());
        if (statementTransformer != null) {
            return statementTransformer;
        }
        DialectMeta key = DialectMeta.createDefault(dialectMeta.getDialectName());
        return maps.get(key.toString());
    }
}
