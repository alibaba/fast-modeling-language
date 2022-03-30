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

package com.aliyun.aliyun.transform.zen.format;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.formatter.FastModelVisitor;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.format.DefaultExpressionVisitor;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.fastmodel.core.formatter.ExpressionFormatter.formatStringLiteral;

/**
 * ZenVisitor
 *
 * @author panguanjing
 * @date 2021/8/16
 */
public class ZenVisitor extends FastModelVisitor {

    public static final String SEPARATOR = " ";

    private final TransformContext context;

    public ZenVisitor(TransformContext context) {
        this.context = context;
    }

    @Override
    public Boolean visitCreateTable(CreateTable node, Integer indent) {
        List<ColumnDefinition> columnDefines = node.getColumnDefines();
        String result = columnDefines.stream().map(columnDefinition ->
        {

            String value = columnDefinition.getColName().getValue();
            String dataType = formatExpression(columnDefinition.getDataType());
            String commentValue = columnDefinition.getCommentValue();
            if (StringUtils.isBlank(commentValue)) {
                return StringUtils.joinWith(SEPARATOR, value, dataType);
            } else {
                return StringUtils.joinWith(SEPARATOR, value, dataType, formatStringLiteral(commentValue));
            }
        }).collect(Collectors.joining("\n"));
        builder.append(result);
        return true;
    }

    @Override
    protected String formatExpression(BaseExpression baseExpression) {
        return new DefaultExpressionVisitor().process(baseExpression);
    }
}
