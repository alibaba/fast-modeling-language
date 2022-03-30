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
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * 基本的删除语句
 *
 * @author panguanjing
 * @date 2020/9/2
 */
@EqualsAndHashCode(callSuper = true)
@Getter
public abstract class BaseDrop extends BaseOperatorStatement {

    private final boolean isExists;

    public BaseDrop(QualifiedName qualifiedName) {
        super(qualifiedName);
        isExists = false;
    }

    public BaseDrop(QualifiedName qualifiedName, boolean isExists) {
        super(qualifiedName);
        this.isExists = isExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitBaseDrop(this, context);
    }
}
