/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.core.tree;

/**
 * fml node
 *
 * @author panguanjing
 * @date 2022/6/7
 */
public abstract class AbstractFmlNode extends AbstractNode {
    public AbstractFmlNode(NodeLocation location) {
        super(location);
    }

    protected AbstractFmlNode() {
        super();
    }

    /**
     * 针对fml的遍历的visitor内容
     *
     * @param visitor
     * @param context
     * @param <R>
     * @param <C>
     * @return
     */
    public abstract <R, C> R accept(AstVisitor<R, C> visitor, C context);

    /**
     * acceptor wrapper
     *
     * @param visitor
     * @param context
     * @param <R>
     * @param <C>
     * @return
     */
    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return (R)accept((AstVisitor)visitor, context);
    }
}
