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

package com.aliyun.fastmodel.ide.open.start.controller;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.compare.CompareStrategy;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.ide.open.start.constants.ApiConstant;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.TransformerFactory;
import com.aliyun.fastmodel.transform.api.compare.CompareContext;
import com.aliyun.fastmodel.transform.api.compare.CompareResult;
import com.aliyun.fastmodel.transform.api.compare.NodeCompareFactory;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 比对的controller
 *
 * @author panguanjing
 * @date 2021/10/30
 */
@RestController
@RequestMapping(ApiConstant.COMPARE)
public class CompareController {

    private final NodeCompareFactory factory;

    private final TransformerFactory transformerFactory;

    public CompareController(NodeCompareFactory factory,
                             TransformerFactory transformerFactory) {
        this.factory = factory;
        this.transformerFactory = transformerFactory;
    }

    @PostMapping(value = "execute")
    public List<String> execute(String dialect, String source, String target) {
        DialectMeta dialectMeta = DialectMeta.getByName(DialectName.getByCode(dialect));
        CompareResult compareResult = factory.compareResult(dialectMeta, source, target,
            CompareContext.builder()
                .compareStrategy(CompareStrategy.INCREMENTAL)
                .build());
        List<BaseStatement> diffStatements = compareResult.getDiffStatements();
        Transformer transformer = transformerFactory.get(dialectMeta);
        return diffStatements.stream().map(baseStatement -> {
            DialectNode transform = transformer.transform(baseStatement);
            return transform;
        }).filter(node -> {
            return node.isExecutable();
        }).map(node -> {
            return node.getNode();
        }).collect(Collectors.toList());
    }
}
