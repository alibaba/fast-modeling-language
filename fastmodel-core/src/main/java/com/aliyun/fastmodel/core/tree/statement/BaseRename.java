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

package com.aliyun.fastmodel.core.tree.statement;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * 基类修改的名称的语句
 *
 * @author panguanjing
 * @date 2020/9/28
 */
@Getter
@Setter
@EqualsAndHashCode(callSuper = false)
public abstract class BaseRename extends BaseOperatorStatement {

    private final QualifiedName target;

    public BaseRename(QualifiedName source, QualifiedName target) {
        super(source);
        Preconditions.checkNotNull(target);
        this.target = target;
    }

    public String getNewIdentifier() {
        if (target == null) {
            return null;
        }
        return target.getSuffix();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitBaseRename(this, context);
    }
}
