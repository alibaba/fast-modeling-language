/*
 * Copyright (c)  2021. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.core.tree.statement.dqc.check;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AbstractFmlNode;
import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * 检查元素内容
 *
 * @author panguanjing
 * @date 2021/6/7
 */
@Getter
public abstract class BaseCheckElement extends AbstractFmlNode {

    protected final Identifier checkName;

    protected final Boolean enforced;

    protected final boolean enable;

    public BaseCheckElement(Identifier checkName, Boolean enforced, boolean enable) {
        this.checkName = checkName;
        this.enforced = enforced;
        this.enable = enable;
    }

    /**
     * return baseExpression
     *
     * @return
     */
    public abstract BaseExpression getBoolExpression();

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitBaseCheckElement(this, context);
    }
}
