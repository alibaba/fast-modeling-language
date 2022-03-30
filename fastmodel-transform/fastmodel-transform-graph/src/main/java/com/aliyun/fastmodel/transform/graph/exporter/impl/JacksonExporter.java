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

package com.aliyun.fastmodel.transform.graph.exporter.impl;

import com.aliyun.fastmodel.transform.graph.domain.FmlGraph;
import com.aliyun.fastmodel.transform.graph.exception.ExportException;
import com.aliyun.fastmodel.transform.graph.exporter.ExportName;
import com.aliyun.fastmodel.transform.graph.exporter.Exporter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.service.AutoService;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/12/15
 */
@AutoService(Exporter.class)
public class JacksonExporter implements Exporter {

    private ObjectMapper objectMapper = new ObjectMapper();

    public JacksonExporter() {

    }

    @Override
    public String exportGraph(FmlGraph graph) {
        try {
            objectMapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
            return objectMapper.writeValueAsString(graph);
        } catch (JsonProcessingException e) {
            throw new ExportException("export graph error", e);
        }
    }

    @Override
    public ExportName getName() {
        return ExportName.JSON;
    }
}
