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

package com.aliyun.fastmodel.compare;

import java.util.List;
import java.util.Optional;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;

/**
 * 比对Node方法
 *
 * @author panguanjing
 * @date 2021/2/2
 */
public abstract class BaseCompareNode<T extends Node> {

    /**
     * 在比对之前对node进行处理
     *
     * @param before
     * @param after
     */
    protected ComparePair prepareCompare(Optional<Node> before, Optional<Node> after) {
        return new ComparePair(before, after);
    }

    /**
     * 执行compare的方法
     *
     * @param before
     * @param after
     * @param compareStrategy
     * @return
     */
    public List<BaseStatement> compareNode(Node before, Node after, CompareStrategy compareStrategy) {
        ComparePair comparePair = prepareCompare(Optional.ofNullable(before), Optional.ofNullable(after));
        List<BaseStatement> statements = compareResult((T)comparePair.getLeft().orElse(null), (T)comparePair.getRight().orElse(null),
            compareStrategy);
        return complete(comparePair, statements);
    }

    /**
     * 完成statement操作内容
     *
     * @param statements
     * @return
     */
    protected List<BaseStatement> complete(ComparePair pair, List<BaseStatement> statements) {
        return statements;
    }

    /**
     * 比对的结果内容
     *
     * @param before   之前的语句
     * @param after    之后的语句
     * @param strategy 比对策略，支持全量和增量模式 {@link CompareStrategy}
     * @return {@link BaseStatement}
     */
    public abstract List<BaseStatement> compareResult(T before, T after, CompareStrategy strategy);

}
