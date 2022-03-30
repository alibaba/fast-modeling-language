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

package com.aliyun.aliyun.transform.zen.parser.tree;

import com.aliyun.aliyun.transform.zen.parser.BaseZenAstVisitor;
import lombok.Getter;

/**
 * brother expression
 *
 * @author panguanjing
 * @date 2021/7/14
 */
@Getter
public class BrotherZenExpression extends BaseZenExpression {

    private final BaseZenExpression left;

    private final BaseZenExpression right;

    public BrotherZenExpression(BaseZenExpression left, BaseZenExpression right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public <R, C> R accept(BaseZenAstVisitor<R, C> visitor, C context) {
        return visitor.visitBrotherZenExpression(this, context);
    }
}
