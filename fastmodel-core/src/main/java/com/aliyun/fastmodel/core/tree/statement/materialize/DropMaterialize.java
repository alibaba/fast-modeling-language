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

package com.aliyun.fastmodel.core.tree.statement.materialize;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.BaseDrop;
import com.aliyun.fastmodel.core.tree.statement.constants.MaterializedType;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * drop 物化视图
 * <p>
 * DSL举例：
 * <pre> DROP MATERIALIZED ut.materialized_name; </pre>
 *
 * @author panguanjing
 * @date 2020/9/21
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class DropMaterialize extends BaseDrop {

    /**
     * 物化类型
     */
    private final MaterializedType materializedType;

    public DropMaterialize(QualifiedName qualifiedName, MaterializedType materializedType) {
        super(qualifiedName);
        this.setStatementType(StatementType.MATERIALIZE);
        this.materializedType = materializedType;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropMaterialize(this, context);
    }
}
