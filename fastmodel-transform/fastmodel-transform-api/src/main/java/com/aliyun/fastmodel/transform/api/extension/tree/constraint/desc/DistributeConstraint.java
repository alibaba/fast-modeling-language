/*
 * Copyright [2024] [name of copyright owner]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import lombok.Getter;

/**
 * distribute key constraint
 *
 * @author panguanjing
 * @date 2023/12/15
 */
@Getter
public class DistributeConstraint extends NonKeyConstraint {

    private final List<Identifier> columns;

    private final Boolean random;

    private final Integer bucket;

    public static final String TYPE = "DISTRIBUTE";

    public DistributeConstraint(List<Identifier> columns, Integer bucket) {
        this(columns, false, bucket);
    }

    public DistributeConstraint(List<Identifier> columns, Boolean random, Integer bucket) {
        super(IdentifierUtil.sysIdentifier(), true, TYPE);
        this.columns = columns;
        this.random = random;
        this.bucket = bucket;
    }

    public DistributeConstraint(boolean random, Integer bucket) {
        this(null, random, bucket);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        ExtensionAstVisitor<R, C> extensionVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return extensionVisitor.visitDistributeKeyConstraint(this, context);
    }
}
