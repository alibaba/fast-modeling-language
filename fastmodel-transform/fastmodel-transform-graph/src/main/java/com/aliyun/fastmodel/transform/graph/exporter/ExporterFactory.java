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

package com.aliyun.fastmodel.transform.graph.exporter;

import java.util.Map;
import java.util.ServiceLoader;

import com.aliyun.fastmodel.transform.graph.exporter.exception.ExporterNotExistException;
import com.google.common.collect.Maps;

/**
 * 导出工厂类
 *
 * @author panguanjing
 * @date 2021/12/15
 */
public class ExporterFactory {

    private static final Map<ExportName, Exporter> EXPORT_NAME_EXPORTER_HASH_MAP = Maps.newHashMapWithExpectedSize(10);

    public ExporterFactory() {
        ServiceLoader<Exporter> load = ServiceLoader.load(Exporter.class);
        for (Exporter exporter : load) {
            EXPORT_NAME_EXPORTER_HASH_MAP.put(exporter.getName(), exporter);
        }
    }

    private static class SingletonHolder {
        private static final ExporterFactory INSTANCE = new ExporterFactory();
    }

    public static ExporterFactory getInstance() {
        return SingletonHolder.INSTANCE;
    }

    /**
     * 获取导出器
     *
     * @param exportName
     * @param <V>        vertex
     * @param <E>        edge
     * @return {@link Exporter}
     */
    public Exporter getExporter(ExportName exportName) {
        Exporter exporter = EXPORT_NAME_EXPORTER_HASH_MAP.get(exportName);
        if (exporter == null) {
            throw new ExporterNotExistException(exportName);
        }
        return exporter;
    }
}
