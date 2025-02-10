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

package com.aliyun.fastmodel.transform.api.extension.tree.constraint;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.CustomConstraint;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import lombok.Getter;

/**
 * adb mysql聚集索引的定义，每种引擎聚集索引的定义都可能存在差异。
 * 所以单独抽取到Adb mysql的包里进行定义
 *
 * @author panguanjing
 * @date 2024/10/8
 */
@Getter
public class ClusteredKeyConstraint extends CustomConstraint {

    private final List<Identifier> columns;

    public ClusteredKeyConstraint(Identifier constraintName, Boolean enable, List<Identifier> columns) {
        super(constraintName, enable, "CLUSTERED_KEY");
        this.columns = columns;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        ExtensionAstVisitor<R, C> extensionVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return extensionVisitor.visitClusteredKeyConstraint(this, context);
    }
}
