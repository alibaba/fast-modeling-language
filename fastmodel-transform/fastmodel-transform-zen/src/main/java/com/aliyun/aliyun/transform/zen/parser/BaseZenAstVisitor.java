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

package com.aliyun.aliyun.transform.zen.parser;

import com.aliyun.aliyun.transform.zen.parser.tree.AtomicZenExpression;
import com.aliyun.aliyun.transform.zen.parser.tree.AttributeZenExpression;
import com.aliyun.aliyun.transform.zen.parser.tree.BaseZenExpression;
import com.aliyun.aliyun.transform.zen.parser.tree.BaseZenNode;
import com.aliyun.aliyun.transform.zen.parser.tree.BrotherZenExpression;
import com.aliyun.fastmodel.core.tree.IAstVisitor;

/**
 * ZenAstVisitor
 *
 * @author panguanjing
 * @date 2021/7/14
 */
public abstract class BaseZenAstVisitor<R, C> implements IAstVisitor<R, C> {

    public R process(BaseZenNode node) {
        return process(node, null);
    }

    public R process(BaseZenNode node, C context) {
        return node.accept(this, context);
    }

    public R visitBaseZenExpression(BaseZenExpression baseZenExpression, C context) {
        return null;
    }

    public R visitBrotherZenExpression(BrotherZenExpression brotherZenExpression, C context) {
        return visitBaseZenExpression(brotherZenExpression, context);
    }

    public R visitAtomicZenExpression(AtomicZenExpression atomicZenExpression, C context) {
        return visitBaseZenExpression(atomicZenExpression, context);
    }

    public R visitAttributeZenExpression(AttributeZenExpression attributeZenExpression, C context) {
        return visitBaseZenExpression(attributeZenExpression, context);
    }
}
