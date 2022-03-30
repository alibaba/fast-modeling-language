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

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.dialect.GenericDialectNode;

/**
 * 语句构建处理
 *
 * @author panguanjing
 * @date 2020/10/16
 */
public interface StatementBuilder<T extends TransformContext> {

    /**
     * special base statement build to DialectNode
     *
     * @param source  source
     * @param context context
     * @return {@link DialectNode}
     */
    default DialectNode build(BaseStatement source, T context) {
        return (DialectNode)buildGenericNode(source, context);
    }

    /**
     * provide generic dialect node
     *
     * @param source
     * @param context
     * @param <R>
     * @return
     */
    default <R extends GenericDialectNode> R buildGenericNode(BaseStatement source, T context) {
        throw new UnsupportedOperationException();
    }
}
