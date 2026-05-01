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

package com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import lombok.Getter;

/**
 * cluster by constraint
 *
 * @author panguanjing
 * @date 2024/1/21
 */
@Getter
public class ClusterNonKeyConstraint extends NonKeyConstraint {

    public static final String TYPE = "Cluster";

    private final List<Identifier> columns;

    public ClusterNonKeyConstraint(Identifier constraintName, Boolean enable, List<Identifier> columns) {
        super(constraintName, enable, TYPE);
        this.columns = columns;
    }

    public ClusterNonKeyConstraint(List<Identifier> columns) {
        super(IdentifierUtil.sysIdentifier(), true, TYPE);
        this.columns = columns;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        ExtensionAstVisitor<R, C> extensionVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return extensionVisitor.visitClusterKeyConstraint(this, context);
    }

}
