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

import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.google.common.base.Preconditions;
import lombok.Getter;

/**
 * BaseSetAliasedName
 *
 * @author panguanjing
 * @date 2021/5/18
 */
@Getter
public class BaseSetAliasedName extends BaseOperatorStatement {

    private final AliasedName aliasedName;

    public BaseSetAliasedName(QualifiedName qualifiedName, AliasedName aliasedName) {
        super(qualifiedName);
        Preconditions.checkNotNull(aliasedName, "aliasedName can't be null");
        this.aliasedName = aliasedName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitBaseSetAliasedName(this, context);
    }
}
