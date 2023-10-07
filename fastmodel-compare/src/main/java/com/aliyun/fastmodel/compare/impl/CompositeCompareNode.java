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

package com.aliyun.fastmodel.compare.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.compare.impl.helper.ComparePair;
import com.aliyun.fastmodel.compare.CompareStrategy;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.BaseCreate;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.google.common.collect.Lists;

/**
 * 复合语句的对比操作
 *
 * @author panguanjing
 * @date 2021/8/31
 */
public class CompositeCompareNode extends BaseCompareNode<CompositeStatement> {

    private final Map<String, BaseCompareNode> singleStatementCompare;

    public CompositeCompareNode(Map<String, BaseCompareNode> singleStatementCompare) {
        this.singleStatementCompare = singleStatementCompare;
    }

    @Override
    protected ComparePair prepareCompare(Optional<Node> before, Optional<Node> after) {
        boolean beforePresent = before.isPresent();
        boolean afterPresent = after.isPresent();
        Optional<Node> left = before;
        Optional<Node> right = after;
        if (beforePresent) {
            Node node = before.get();
            if (!(node instanceof CompositeStatement) && node instanceof BaseStatement) {
                left = Optional.of(new CompositeStatement(Arrays.asList((BaseStatement)node)));
            }
        }
        if (afterPresent) {
            Node node = after.get();
            if (!(node instanceof CompositeStatement) && node instanceof BaseStatement) {
                right = Optional.of(new CompositeStatement(Arrays.asList((BaseStatement)node)));
            }
        }
        return new ComparePair(left, right);
    }

    @Override
    public List<BaseStatement> compareResult(CompositeStatement before,
                                             CompositeStatement after,
                                             CompareStrategy strategy) {

        List<BaseStatement> result = getBaseStatementsIfOneNull(before, after, strategy);
        if (result != null) {return result;}
        result = new ArrayList<>();
        //将传入的statement都改为可以排序的列表
        List<BaseStatement> beforeList = Lists.newArrayList(before.getStatements());
        List<BaseStatement> afterList = Lists.newArrayList(after.getStatements());
        List<QualifiedName> beforeQualifiedName = getQualifiedNames(beforeList);
        List<QualifiedName> afterQualifiedName = getQualifiedNames(afterList);
        int beforeSize = beforeList.size();
        int afterSize = afterList.size();
        int size = beforeSize > afterSize ? afterSize : beforeSize;

        //如果before集合大小，大于after的集合大小，那么将before集合按照after的集合的名字进行排序
        if (beforeSize >= afterSize) {
            //排序，将相同的名字的操作进行处理
            sort(beforeList, afterQualifiedName);
            beforeQualifiedName = getQualifiedNames(beforeList);
            sort(afterList, beforeQualifiedName);
        } else {
            //排序，将相同的名字的操作进行处理
            //如果before集合大小，小于after的集合大小，那么将after集合按照before的集合的名字进行排序
            sort(afterList, beforeQualifiedName);
            afterQualifiedName = getQualifiedNames(afterList);
            sort(beforeList, afterQualifiedName);
        }
        for (int i = 0; i < size; i++) {
            BaseStatement baseStatement = beforeList.get(i);
            BaseStatement afterStatement = afterList.get(i);
            BaseCompareNode baseCompareNode = singleStatementCompare.get(baseStatement.getClass().getName());
            //if compare node is null,  skip
            if (baseCompareNode == null) {
                continue;
            }
            result.addAll(baseCompareNode.compareResult(baseStatement, afterStatement, strategy));
        }
        if (size < beforeSize) {
            for (int start = size; start < beforeSize; start++) {
                BaseStatement baseStatement = beforeList.get(start);
                BaseCompareNode baseCompareNode = singleStatementCompare.get(baseStatement.getClass().getName());
                //if compare node is null,  skip
                if (baseCompareNode == null) {
                    continue;
                }
                List compareResult = baseCompareNode.compareResult(baseStatement, null, strategy);
                result.addAll(compareResult);
            }
        } else if (size < afterSize) {
            for (int start = size; start < afterSize; start++) {
                BaseStatement baseStatement = afterList.get(start);
                BaseCompareNode baseCompareNode = singleStatementCompare.get(baseStatement.getClass().getName());
                //if compare node is null,  skip
                if (baseCompareNode == null) {
                    continue;
                }
                result.addAll(baseCompareNode.compareResult(null, baseStatement, strategy));
            }
        }
        return result;
    }

    private List<QualifiedName> getQualifiedNames(List<BaseStatement> beforeList) {
        return beforeList.stream()
            .filter(statement -> statement instanceof BaseCreate).map(
                baseStatement -> {
                    BaseCreate b = (BaseCreate)baseStatement;
                    return b.getQualifiedName();
                }
            )
            .collect(Collectors.toList());
    }

    private void sort(List<BaseStatement> beforeList, List<QualifiedName> afterQualifiedName) {
        Collections.sort(beforeList,
            Comparator.comparing(item -> {
                if (!(item instanceof BaseCreate)) {
                    return Integer.MAX_VALUE;
                }
                BaseCreate baseCreate = (BaseCreate)item;
                int index = afterQualifiedName.indexOf(baseCreate.getQualifiedName());
                if (index == -1) {
                    return Integer.MAX_VALUE;
                }
                return index;
            }));
    }

    private List<BaseStatement> getBaseStatementsIfOneNull(CompositeStatement before, CompositeStatement after,
                                                           CompareStrategy strategy) {
        if (before == null) {
            List<BaseStatement> result = new ArrayList<>();
            List<BaseStatement> statements = after.getStatements();
            for (BaseStatement statement : statements) {
                BaseCompareNode baseCompareNode = singleStatementCompare.get(statement.getClass().getName());
                List<BaseStatement> list = baseCompareNode.compareResult(before, statement, strategy);
                result.addAll(list);
            }
            return result;
        }
        if (after == null) {
            List<BaseStatement> result = new ArrayList<>();
            List<BaseStatement> statements = before.getStatements();
            for (BaseStatement statement : statements) {
                BaseCompareNode baseCompareNode = singleStatementCompare.get(statement.getClass().getName());
                List<BaseStatement> list = baseCompareNode.compareResult(statement, after, strategy);
                result.addAll(list);
            }
            return result;
        }
        return null;
    }
}
