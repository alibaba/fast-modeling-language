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

package com.aliyun.fastmodel.transform.fml.format;

import java.util.List;

import com.aliyun.fastmodel.core.formatter.FastModelVisitor;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.transform.fml.context.FmlTransformContext;

import static java.util.stream.Collectors.joining;

/**
 * FML visitor
 *
 * @author panguanjing
 * @date 2020/12/14
 */
public class FmlVisitor extends FastModelVisitor {

    private final FmlTransformContext context;

    public FmlVisitor(FmlTransformContext context) {
        this.context = context;
    }

    @Override
    protected BaseDataType convert(BaseDataType dataType) {
        if (context.getDataTypeTransformer() != null) {
            return context.getDataTypeTransformer().convert(dataType);
        }
        return super.convert(dataType);
    }

    @Override
    protected String getCode(QualifiedName qualifiedName) {
        return formatName(QualifiedName.of(qualifiedName.getSuffix()));
    }

    @Override
    protected String formatProperty(List<Property> properties) {
        if (properties == null || properties.isEmpty()) {
            return "";
        }
        return properties.stream().filter(
            x -> !context.isFilter(x.getName())
        ).map(
            x -> formatStringLiteral(x.getName()) + "=" + formatStringLiteral(x.getValue())
        ).collect(joining(","));
    }

    @Override
    protected String formatExpression(BaseExpression baseExpression) {
        return new FmlExpressionVisitor().process(baseExpression);
    }

}
