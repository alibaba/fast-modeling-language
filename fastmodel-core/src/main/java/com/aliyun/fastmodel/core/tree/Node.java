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

package com.aliyun.fastmodel.core.tree;

import java.util.List;

/**
 * 抽象的节点
 *
 * @author panguanjing
 * @date 2020/10/29
 */
public interface Node {
    /**
     * 接受处理
     *
     * @param visitor 访问者
     * @param context 上下文
     * @return 遍历结果
     */
    <R, C> R accept(IAstVisitor<R, C> visitor, C context);

    /**
     * 获取子节点内容
     *
     * @return 子节点
     */
    List<? extends Node> getChildren();
}
