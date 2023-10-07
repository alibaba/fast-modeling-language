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

package com.aliyun.fastmodel.transform.api.dialect.transform;

import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import lombok.Getter;

/**
 * Data to Object
 *
 * @author panguanjing
 * @date 2021/7/26
 */
@Getter
public class DialectTransformParam {

    private final DialectNode sourceNode;

    private final DialectMeta sourceMeta;

    private final DialectMeta targetMeta;

    private final ReverseContext reverseContext;

    private final TransformContext transformContext;

    protected DialectTransformParam(DialectTransformParamBuilder builder) {
        sourceNode = builder.sourceNode;
        sourceMeta = builder.sourceMeta;
        targetMeta = builder.targetMeta;
        transformContext = builder.transformContext;
        reverseContext = builder.reverseContext;
    }

    public static DialectTransformParamBuilder builder() {
        return new DialectTransformParamBuilder();
    }

    public static class DialectTransformParamBuilder {

        private DialectNode sourceNode;

        private DialectMeta sourceMeta;

        private DialectMeta targetMeta;

        private ReverseContext reverseContext;

        private TransformContext transformContext;

        public DialectTransformParamBuilder sourceNode(DialectNode sourceNode) {
            this.sourceNode = sourceNode;
            return this;
        }

        public DialectTransformParamBuilder sourceMeta(DialectMeta sourceMeta) {
            this.sourceMeta = sourceMeta;
            return this;
        }

        public DialectTransformParamBuilder targetMeta(DialectMeta targetMeta) {
            this.targetMeta = targetMeta;
            return this;
        }

        public DialectTransformParamBuilder reverseContext(ReverseContext reverseContext) {
            this.reverseContext = reverseContext;
            return this;
        }

        public DialectTransformParamBuilder transformContext(TransformContext transformContext) {
            this.transformContext = transformContext;
            return this;
        }

        public DialectTransformParam build() {
            return new DialectTransformParam(this);
        }
    }

}
