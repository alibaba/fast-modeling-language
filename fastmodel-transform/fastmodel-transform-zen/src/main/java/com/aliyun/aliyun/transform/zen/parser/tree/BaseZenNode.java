/*
 * Copyright (c)  2021. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.aliyun.transform.zen.parser.tree;

import com.aliyun.aliyun.transform.zen.parser.BaseZenAstVisitor;
import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;

/**
 * BaseZenNode
 *
 * @author panguanjing
 * @date 2021/7/14
 */
public interface BaseZenNode extends Node {

    /**
     * accept
     *
     * @param visitor 访问者
     * @param context 上下文
     * @param <R>
     * @param <C>
     * @return
     */
    @Override
    default <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return (R)accept((BaseZenAstVisitor)visitor, context);
    }

    /**
     * 接受处理
     *
     * @param visitor 访问者
     * @param context 上下文
     * @return 遍历结果
     */
    <R, C> R accept(BaseZenAstVisitor<R, C> visitor, C context);
}
