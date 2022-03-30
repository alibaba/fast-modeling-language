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

import com.aliyun.fastmodel.ide.open.start.constants.ApiConstant;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.dialect.transform.DialectTransform;
import com.aliyun.fastmodel.transform.api.dialect.transform.DialectTransformParam;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * TransformController
 *
 * @author panguanjing
 * @date 2021/8/16
 */
@RestController
@RequestMapping(value = ApiConstant.TRANSFORM)
public class TransformController {

    @PostMapping
    @ResponseBody
    public String transform(String source, String target, String content) {
        DialectTransformParam build = DialectTransformParam.builder()
            .sourceMeta(DialectMeta.getByNameAndVersion(source, DialectMeta.DEFAULT_VERSION))
            .sourceNode(new DialectNode(content))
            .targetMeta(DialectMeta.getByNameAndVersion(target, DialectMeta.DEFAULT_VERSION))
            .transformContext(TransformContext.builder().appendSemicolon(true).build())
            .build();
        DialectNode transform = DialectTransform.transform(build);
        return transform.getNode();
    }
}
