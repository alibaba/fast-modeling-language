/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.parser.visitor;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.transform.hologres.parser.tree.BeginWork;
import com.aliyun.fastmodel.transform.hologres.parser.tree.CommitWork;
import com.aliyun.fastmodel.transform.hologres.parser.tree.datatype.ArrayBounds;
import com.aliyun.fastmodel.transform.hologres.parser.tree.datatype.HologresArrayDataType;
import com.aliyun.fastmodel.transform.hologres.parser.tree.datatype.HologresGenericDataType;

/**
 * visit begin work
 *
 * @author panguanjing
 * @date 2022/6/9
 */
public interface HologresVisitor<R, C> extends IAstVisitor<R, C> {

    /**
     * visit begin work
     *
     * @param beginWork
     * @param context
     * @return
     */
    default R visitBeginWork(BeginWork beginWork, C context) {
        return visitStatement(beginWork, context);
    }

    /**
     * visit commit work
     *
     * @param commitWork
     * @param context
     * @return
     */
    default R visitCommitWork(CommitWork commitWork, C context) {
        return visitStatement(commitWork, context);
    }

    /**
     * array bounds
     *
     * @param arrayBounds
     * @param context
     * @return
     */
    default R visitArrayBounds(ArrayBounds arrayBounds, C context) {
        return visitNode(arrayBounds, context);
    }

    /**
     * visit hologres generic DataType
     *
     * @param hologresGenericDataType
     * @param context
     * @return
     */
    default R visitHologresGenericDataType(HologresGenericDataType hologresGenericDataType, C context) {
        return visitGenericDataType(hologresGenericDataType, context);
    }

    /**
     * visit hologres Array DataType
     *
     * @param hologresArrayDataType
     * @param context
     * @return
     */
    default R visitHologresArrayDataType(HologresArrayDataType hologresArrayDataType, C context) {
        return visitDataType(hologresArrayDataType, context);
    }
}
