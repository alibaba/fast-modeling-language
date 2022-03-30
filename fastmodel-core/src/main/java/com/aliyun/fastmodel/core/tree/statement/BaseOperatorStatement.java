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

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * 基类statement, 语句中必须存在一个标示进行处理。可以脱离指定的业务板块中运行。
 * <p>
 * 描述信息，每个statement必须有一个QualifiedName
 *
 * @author panguanjing
 * @date 2020/9/9
 */
@Getter
@Setter
@EqualsAndHashCode(callSuper = false)
public abstract class BaseOperatorStatement extends BaseStatement {

    private QualifiedName qualifiedName;

    public BaseOperatorStatement(QualifiedName qualifiedName) {
        this(null, null, qualifiedName);
    }

    public BaseOperatorStatement(NodeLocation nodeLocation, String origin, QualifiedName qualifiedName) {
        super(nodeLocation, origin);
        this.qualifiedName = qualifiedName;
    }

    /**
     * get the business unit
     *
     * @return unit
     */
    public String getBusinessUnit() {
        return qualifiedName.getFirstIfSizeOverOne();
    }

    /**
     * 修改业务单元
     *
     * @param businessUnit 业务单元
     */
    public void setBusinessUnit(String businessUnit) {
        String origin = getBusinessUnit();
        if (origin == null) {
            List<Identifier> identifiers = Lists.newArrayList(new Identifier(businessUnit));
            identifiers.addAll(qualifiedName.getOriginalParts());
            setQualifiedName(QualifiedName.of(identifiers));
        } else {
            QualifiedName qualifiedName = QualifiedName.of(businessUnit, getIdentifier());
            setQualifiedName(qualifiedName);
        }
    }

    /**
     * get the identifier
     *
     * @return identifier
     */
    public String getIdentifier() {
        return qualifiedName.getSuffixPath();
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitBaseOperatorStatement(this, context);
    }
}

