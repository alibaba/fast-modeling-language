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

package com.aliyun.fastmodel.transform.api;

import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.PropertyConverter;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.google.common.base.Preconditions;

/**
 * 转换者, 用于从DSL的statement转换为指定引擎执行的操作。
 * <p>
 * 引擎的列举：
 * <pre>
 *  <b>MaxCompute</b>
 *  <b>Hive</b>
 *  <b>Hologres</b>
 * </pre>
 * <pre>{@code
 *   Transformer<Statement> transformer = TransformFactory.getInstance().get(EngineMeta.getMaxCompute());
 * }
 * </pre>
 *
 * @author panguanjing
 * @date 2020/10/16
 */
public interface Transformer<T extends Node> extends PropertyConverter {

    /**
     * 转换器, 核心的内容处理，默认不存在
     *
     * @param source  来源
     * @param context 上下文
     * @return T 结果
     */
    default DialectNode transform(T source, TransformContext context) {
        throw new UnsupportedOperationException("unSupported transform");
    }

    /**
     * 支持默认的转换
     *
     * @param source
     * @return
     */
    default DialectNode transform(T source) {
        Preconditions.checkNotNull(source, "source can't be null");
        return transform(source, TransformContext.builder().build());
    }

    /**
     * 逆向转换处理
     *
     * @param dialectNode 结果信息
     * @param context     context {@link TransformContext} 使用builder的方式进行构建
     * @return T
     */
    default T reverse(DialectNode dialectNode, ReverseContext context) {
        throw new UnsupportedOperationException("unSupported Reverse");
    }

    /**
     * 默认转换方法处理
     *
     * @param dialectNode
     * @return
     */
    default T reverse(DialectNode dialectNode) {
        Preconditions.checkNotNull(dialectNode);
        return reverse(dialectNode, ReverseContext.builder().build());
    }

    /**
     * 将client dto 转为 核心模型
     *
     * @param table
     * @param context 上下文处理
     * @return
     */
    default Node reverseTable(Table table, ReverseContext context) {
        throw new UnsupportedOperationException("unSupported Reverse with tableClient");
    }

    /**
     * 将client dto 转为 核心模型
     *
     * @param table
     * @return
     */
    default Node reverseTable(Table table) {
        return reverseTable(table, ReverseContext.builder().build());
    }

    /**
     * 将client dto 转为 核心模型
     *
     * @param table
     * @param context 上下文处理
     * @return
     */
    default Table transformTable(Node table, TransformContext context) {
        throw new UnsupportedOperationException("unsupported transform table");
    }

}
