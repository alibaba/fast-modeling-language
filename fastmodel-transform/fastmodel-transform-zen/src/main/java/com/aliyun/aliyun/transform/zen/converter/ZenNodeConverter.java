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

package com.aliyun.aliyun.transform.zen.converter;

import java.util.List;

import com.aliyun.aliyun.transform.zen.parser.tree.BaseZenNode;
import com.aliyun.fastmodel.core.tree.Node;

/**
 * Fml节点的转换器
 *
 * @author panguanjing
 * @date 2021/7/15
 */
public interface ZenNodeConverter<R extends Node, C> {

    /**
     * convert zen node with context
     *
     * @param baseZenNode
     * @param context
     * @return
     */
    public List<R> convert(BaseZenNode baseZenNode, C context);

    /**
     * convert zenNode to node
     *
     * @param baseZenNode
     * @return
     */
    public default List<R> convert(BaseZenNode baseZenNode) {
        return convert(baseZenNode, null);
    }

}
