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

package com.aliyun.fastmodel.transform.api.compare;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import org.apache.commons.lang3.StringUtils;

/**
 * 节点比对
 *
 * @author panguanjing
 * @date 2021/8/29
 */
public interface NodeCompare {

    /**
     * 比较的处理
     *
     * @param before 比对之前的节点
     * @param after  比对之后的节点
     * @return {@link BaseStatement}
     */
    default List<BaseStatement> compare(DialectNode before, DialectNode after) {
        return compare(before, after, CompareContext.builder().build());
    }

    /**
     * 比较处理
     *
     * @param before
     * @param after
     * @param context
     * @return
     */
    default List<BaseStatement> compare(DialectNode before, DialectNode after, CompareContext context) {
        CompareResult compareResult = compareResult(before,after,context);
        return compareResult.getDiffStatements();
    }

    /**
     * 计算结果信息
     *
     * @param before  比对之前节点
     * @param after   比对之后的节点
     * @param context 比对上下文
     * @return {@link CompareResult}
     */
    CompareResult compareResult(DialectNode before, DialectNode after, CompareContext context);

    /**
     * Node is not Null
     * @param dialectNode 方言节点
     * @return notNull
     */
    default boolean isNotNull(DialectNode dialectNode) {
        return dialectNode != null && StringUtils.isNotBlank(dialectNode.getNode());
    }

}
