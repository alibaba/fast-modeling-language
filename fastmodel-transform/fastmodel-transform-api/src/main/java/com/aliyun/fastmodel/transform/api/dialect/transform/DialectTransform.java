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

import java.util.Objects;

import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.TransformerFactory;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.datatype.DataTypeConverter;
import com.aliyun.fastmodel.transform.api.datatype.DataTypeConverterFactory;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.google.common.base.Preconditions;

/**
 * 转换服务
 *
 * @author panguanjing
 * @date 2021/7/26
 */
public class DialectTransform {

    /**
     * 支持将一个方言的code转换为另外一个方言的code的处理
     *
     * @param dialectTransformParam
     * @return {@link DialectNode}
     */
    public static DialectNode transform(DialectTransformParam dialectTransformParam) {
        Preconditions.checkNotNull(dialectTransformParam, "param can't be null");
        DialectMeta sourceMeta = dialectTransformParam.getSourceMeta();
        DialectMeta targetMeta = dialectTransformParam.getTargetMeta();
        DialectNode sourceNode = dialectTransformParam.getSourceNode();
        Preconditions.checkNotNull(sourceMeta, "source dialect meta can't be null");
        Preconditions.checkNotNull(targetMeta, "target dialect meta can't be null");
        Preconditions.checkNotNull(sourceNode, "source node can't be null");

        //if source meta equal target meta
        if (Objects.equals(sourceMeta, targetMeta)) {
            return dialectTransformParam.getSourceNode();
        }
        Transformer sourceTransformer = TransformerFactory.getInstance().get(sourceMeta);
        if (sourceTransformer == null) {
            throw new UnsupportedOperationException(
                "can't find source transformer with meta:" + sourceMeta);
        }
        Transformer targetTransformer = TransformerFactory.getInstance().get(targetMeta);
        if (targetTransformer == null) {
            throw new UnsupportedOperationException(
                "can't find target transformer with meta:" + targetMeta);
        }
        Node reverse = sourceTransformer.reverse(sourceNode, dialectTransformParam.getReverseContext());
        if (reverse == null) {
            throw new UnsupportedOperationException(
                "unsupported reverse the sourceNode:" + dialectTransformParam.getSourceNode());
        }
        TransformContext transformContext = dialectTransformParam.getTransformContext();
        if (transformContext == null) {
            transformContext = TransformContext.builder().build();
        }
        if (transformContext.getDataTypeTransformer() == null) {
            DataTypeConverter dataTypeTransformer = DataTypeConverterFactory.getInstance()
                .get(sourceMeta, targetMeta);
            transformContext.setDataTypeTransformer(dataTypeTransformer);
        }
        return targetTransformer.transform(reverse, transformContext);
    }

}
