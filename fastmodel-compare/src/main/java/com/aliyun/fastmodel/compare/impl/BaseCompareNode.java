/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.compare.impl;

import java.util.List;
import java.util.Optional;

import com.aliyun.fastmodel.compare.impl.helper.ComparePair;
import com.aliyun.fastmodel.compare.CompareStrategy;
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
