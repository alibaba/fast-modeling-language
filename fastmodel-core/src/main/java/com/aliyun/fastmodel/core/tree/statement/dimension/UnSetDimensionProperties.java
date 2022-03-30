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

package com.aliyun.fastmodel.core.tree.statement.dimension;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.BaseUnSetProperties;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;

/**
 * 去除数据域属性
 *
 * @author panguanjing
 * @date 2020/9/7
 */
public class UnSetDimensionProperties extends BaseUnSetProperties {

    public UnSetDimensionProperties(QualifiedName qualifiedName, List<String> propertyList) {
        super(qualifiedName, propertyList);
        setStatementType(StatementType.DIMENSION);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitBaseUnSetProperties(this, context);
    }
}
