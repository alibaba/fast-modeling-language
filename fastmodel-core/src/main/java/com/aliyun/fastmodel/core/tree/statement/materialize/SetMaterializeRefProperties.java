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

import java.util.List;
import java.util.Map;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.BaseSetProperties;
import com.aliyun.fastmodel.core.tree.statement.constants.MaterializedType;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * 修改物化名字的语句
 * <p>
 * DSL举例：
 * <pre>
 *         alter materialized table ut.wh references (abc, bcd) engine abc set engineproperties('abc' = 'bcd')
 *         </pre>
 *
 * @author panguanjing
 * @date 2020/9/21
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class SetMaterializeRefProperties extends BaseSetProperties {
    /**
     * reference
     */
    private final List<QualifiedName> references;

    /**
     * 物化类型
     */
    private final MaterializedType materializedType;

    /**
     * 修改后的引擎
     */
    private final Identifier engine;

    public SetMaterializeRefProperties(QualifiedName qualifiedName,
                                       List<Property> propertyList,
                                       List<QualifiedName> references, MaterializedType materializedType,
                                       Identifier engine) {
        super(qualifiedName, propertyList);
        this.references = references;
        this.materializedType = materializedType;
        this.engine = engine;
        this.setStatementType(StatementType.MATERIALIZE);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSetMaterializeRefProperties(this, context);
    }
}
