/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.postgresql.parser.tree;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.transform.postgresql.parser.visitor.PostgreSQLVisitor;

/**
 * begin work
 *
 * @author panguanjing
 * @date 2022/6/9
 */
public class BeginWork extends BaseStatement {

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        PostgreSQLVisitor<R, C> hologresVisitor = (PostgreSQLVisitor<R, C>)visitor;
        return hologresVisitor.visitBeginWork(this, context);
    }
}
