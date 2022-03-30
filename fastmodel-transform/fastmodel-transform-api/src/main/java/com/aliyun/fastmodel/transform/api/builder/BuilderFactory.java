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

package com.aliyun.fastmodel.transform.api.builder;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;

/**
 * Builder Factory的工厂
 *
 * @author panguanjing
 * @date 2020/10/16
 */
public class BuilderFactory {

    public static final String FORMAT = "[%s]-[%s]";

    private static final BuilderFactory FACTORY = new BuilderFactory();

    private final Map<String, StatementBuilder> map = new HashMap<>();

    private BuilderFactory() {
        ServiceLoader<StatementBuilder> load = ServiceLoader.load(
            StatementBuilder.class,
            BuilderFactory.class.getClassLoader());
        for (StatementBuilder statementBuilder : load) {
            BuilderAnnotation annotation = statementBuilder.getClass().getDeclaredAnnotation(
                BuilderAnnotation.class);
            DialectMeta dialectMeta = new DialectMeta(annotation.dialect(), annotation.version());
            Class<?>[] values = annotation.values();
            for (Class<?> v : values) {
                String key = String.format(FORMAT, dialectMeta, v.getName());
                map.put(key, statementBuilder);
            }
        }
    }

    /**
     * 使用单例操作内容
     *
     * @return
     */
    public static BuilderFactory getInstance() {
        return FACTORY;
    }

    /**
     * getBuilder
     * 1. 先根据用于定制的statement进行处理。
     * 2. 如果定制的statement builder找不到，那么再从默认的超类中进行获取
     * 3. 如果传入的dialectMeta中的内容还是找不到，那么取默认的方言信息进行返回
     *
     * @param source 获取制定的builder
     * @return StatementBuilder
     */
    public StatementBuilder getBuilder(BaseStatement source,
                                       DialectMeta dialectMeta) {
        String key = String.format(FORMAT, dialectMeta.toString(),
            source.getClass().getName());
        StatementBuilder statementBuilder = map.get(key);
        if (statementBuilder != null) {
            return statementBuilder;
        }
        key = String.format(FORMAT, dialectMeta, BaseStatement.class.getName());
        statementBuilder = map.get(key);
        if (statementBuilder != null) {
            return statementBuilder;
        }
        key = String.format(FORMAT, DialectMeta.createDefault(dialectMeta.getName()), BaseStatement.class.getName());
        return map.get(key);
    }
}
