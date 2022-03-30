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

package com.aliyun.aliyun.transform.zen.converter;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

import com.aliyun.fastmodel.converter.util.GenericTypeUtil;
import com.aliyun.fastmodel.core.tree.Node;

/**
 * zenNode Converter Factory
 *
 * @author panguanjing
 * @date 2021/7/17
 */
public class ZenNodeConverterFactory {

    protected static Map<String, ZenNodeConverter> maps = new ConcurrentHashMap<>();

    public static final ZenNodeConverterFactory INSTANCE = new ZenNodeConverterFactory();

    private ZenNodeConverterFactory() {
        init();
    }

    public void init() {
        ServiceLoader<ZenNodeConverter> load = ServiceLoader.load(ZenNodeConverter.class);
        for (ZenNodeConverter transformer : load) {
            Class superClassGenericType = GenericTypeUtil.getSuperInterfaceGenericType(transformer.getClass(), 0);
            maps.put(superClassGenericType.getName(), transformer);
        }
    }

    public static ZenNodeConverterFactory getInstance() {
        return INSTANCE;
    }

    public ZenNodeConverter create(Class<? extends Node> nodeClass) {
        ZenNodeConverter statementConverter = maps.get(nodeClass.getName());
        if (statementConverter == null) {
            return maps.get(nodeClass.getSuperclass().getName());
        } else {
            return statementConverter;
        }
    }

}
