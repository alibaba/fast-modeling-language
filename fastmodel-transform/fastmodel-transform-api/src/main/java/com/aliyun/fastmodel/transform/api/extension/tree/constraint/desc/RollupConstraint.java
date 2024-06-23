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

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import lombok.Getter;

/**
 * roll up constraint
 * ROLLUP (rollup_name (column_name1, column_name2, ...)
 * [FROM from_index_name]
 * [PROPERTIES ("key" = "value", ...)],...)
 *
 * @author panguanjing
 * @date 2023/12/15
 */
@Getter
public class RollupConstraint extends NonKeyConstraint {

    public static final String TYPE = "Rollup";

    private final List<RollupItem> rollupItemList;

    public RollupConstraint(List<RollupItem> rollupItemList) {
        super(IdentifierUtil.sysIdentifier(), true, TYPE);
        this.rollupItemList = rollupItemList;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        ExtensionAstVisitor<R, C> starRocksAstVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return starRocksAstVisitor.visitRollupConstraint(this, context);
    }
}
