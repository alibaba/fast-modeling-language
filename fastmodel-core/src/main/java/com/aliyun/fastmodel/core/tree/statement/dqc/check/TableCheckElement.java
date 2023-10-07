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

package com.aliyun.fastmodel.core.tree.statement.dqc.check;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * 表级约束内容处理
 *
 * @author panguanjing
 * @date 2021/6/7
 */
@Getter
public class TableCheckElement extends BaseCheckElement {

    private final BaseExpression boolExpression;

    public TableCheckElement(TableCheckElementBuilder builder) {
        super(builder.checkerName, builder.enforced, builder.enable);
        boolExpression = builder.boolExpression;
    }

    public static TableCheckElementBuilder builder() {
        return new TableCheckElementBuilder();
    }

    public static class TableCheckElementBuilder {

        private Identifier checkerName;

        private Boolean enforced = false;

        private Boolean enable = true;

        private BaseExpression boolExpression;

        public TableCheckElementBuilder enforced(boolean enforced) {
            this.enforced = enforced;
            return this;
        }

        public TableCheckElementBuilder enable(boolean enable) {
            this.enable = enable;
            return this;
        }

        public TableCheckElementBuilder expression(BaseExpression boolExpression) {
            this.boolExpression = boolExpression;
            return this;
        }

        public TableCheckElementBuilder checkerName(Identifier checkerName) {
            this.checkerName = checkerName;
            return this;
        }

        public TableCheckElement build() {
            return new TableCheckElement(this);
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitTableCheckElement(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(boolExpression);
    }
}
