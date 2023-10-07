/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.parser.tree.datatype;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.hologres.parser.visitor.HologresVisitor;
import com.google.common.collect.ImmutableList;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * array bounds
 *
 * @author panguanjing
 * @date 2022/6/9
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class ArrayBounds extends AbstractNode {
    /**
     * 可空
     */
    private final Integer index;

    public ArrayBounds(Integer index) {
        this.index = index;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        HologresVisitor<R, C> hologresVisitor = (HologresVisitor<R, C>)visitor;
        return hologresVisitor.visitArrayBounds(this, context);
    }
}
