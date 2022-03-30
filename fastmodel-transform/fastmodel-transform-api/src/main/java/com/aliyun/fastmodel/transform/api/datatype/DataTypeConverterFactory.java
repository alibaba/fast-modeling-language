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

package com.aliyun.fastmodel.transform.api.datatype;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;

/**
 * Factory
 *
 * @author panguanjing
 * @date 2021/8/13
 */
public class DataTypeConverterFactory {
    private static final DataTypeConverterFactory INSTANCE = new DataTypeConverterFactory();
    public static final String FORMAT = "%s>%s";

    private Map<String, DataTypeConverter> transformerMap = new HashMap<>();

    /**
     * 私有构造函数，只在内容进行调用
     */
    private DataTypeConverterFactory() {
        ServiceLoader<DataTypeConverter> load = ServiceLoader.load(DataTypeConverter.class);
        for (DataTypeConverter transformer : load) {
            String dialectKey = String.format(FORMAT, transformer.getSourceDialect(), transformer.getTargetDialect());
            transformerMap.put(dialectKey, transformer);
            //default key put
            String defaultKey = String.format(FORMAT,
                transformer.getSourceDialect().getName(),
                transformer.getTargetDialect().getName());
            transformerMap.put(defaultKey, transformer);
        }
    }

    public static DataTypeConverterFactory getInstance() {
        return INSTANCE;
    }

    public DataTypeConverter get(DialectMeta source, DialectMeta target) {
        String f = String.format(FORMAT, source, target);
        DataTypeConverter dataTypeTransformer = transformerMap.get(f);
        if (dataTypeTransformer == null) {
            return transformerMap.get(String.format(FORMAT, source.getName(), target.getName()));
        }
        return dataTypeTransformer;
    }
}
