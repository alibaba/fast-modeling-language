/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.parser.tree;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.transform.hologres.parser.visitor.HologresVisitor;

/**
 * commit work
 *
 * @author panguanjing
 * @date 2022/6/9
 */
public class CommitWork extends BaseStatement {

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        HologresVisitor<R, C> hologresVisitor = (HologresVisitor<R, C>)visitor;
        return hologresVisitor.visitCommitWork(this, context);
    }
}
