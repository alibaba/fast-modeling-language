/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.builder.merge;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.transform.api.builder.merge.exception.MergeException;

/**
 * merge builder
 *
 * @author panguanjing
 * @date 2022/6/29
 */
public interface MergeBuilder {

    /**
     * 获取合并的主statement
     *
     * @param baseStatements
     * @return
     * @throws {@see MergeException}
     */
    BaseStatement getMainStatement(List<BaseStatement> baseStatements) throws MergeException;

    /**
     * merge statement
     *
     * @param mainStatement
     * @param other
     * @return
     */
    MergeResult mergeStatement(BaseStatement mainStatement, BaseStatement other);

    /**
     * merge composite statement
     *
     * @param collection
     * @return BaseStatement
     */
    default BaseStatement merge(CompositeStatement collection) {
        List<BaseStatement> statements = collection.getStatements();
        BaseStatement mainStatement = getMainStatement(statements);
        if (mainStatement == null) {
            return collection;
        }
        List<BaseStatement> other = statements.stream().filter(
            x -> x.getClass() != mainStatement.getClass()
        ).collect(Collectors.toList());
        BaseStatement resultStatement = mainStatement;
        for (BaseStatement statement : other) {
            MergeResult mergeResult = mergeStatement(resultStatement, statement);
            if (!mergeResult.isMergeSuccess()) {
                continue;
            } else {
                resultStatement = mergeResult.getStatement();
            }
        }
        return resultStatement;
    }

}
