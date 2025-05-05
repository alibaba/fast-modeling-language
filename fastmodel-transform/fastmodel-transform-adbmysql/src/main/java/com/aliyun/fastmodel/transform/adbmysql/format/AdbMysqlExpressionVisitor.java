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

package com.aliyun.fastmodel.transform.adbmysql.format;

import java.util.List;

import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.LambdaExpression;
import com.aliyun.fastmodel.transform.adbmysql.parser.tree.AdbMysqlDataTypeName;
import com.aliyun.fastmodel.transform.adbmysql.parser.tree.datatype.AdbMysqlPointDataType;
import com.aliyun.fastmodel.transform.adbmysql.parser.visitor.AdbMysqlAstVisitor;
import com.aliyun.fastmodel.transform.api.format.DefaultExpressionVisitor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * adb mysql expression visitor
 *
 * @author panguanjing
 * @date 2024/10/7
 */
public class AdbMysqlExpressionVisitor extends DefaultExpressionVisitor implements AdbMysqlAstVisitor<String, Void> {

    @Override
    public String visitGenericDataType(GenericDataType node, Void context) {
        IDataTypeName typeName = node.getTypeName();
        StringBuilder result = new StringBuilder();
        if (!StringUtils.equalsIgnoreCase(typeName.getValue(), AdbMysqlDataTypeName.ARRAY.getValue())) {
            return super.visitGenericDataType(node, context);
        }
        result.append(AdbMysqlDataTypeName.ARRAY.getValue());
        if (CollectionUtils.isNotEmpty(node.getArguments())) {
            List<DataTypeParameter> arguments = node.getArguments();
            DataTypeParameter first = arguments.get(0);
            result.append("<");
            String process = process(first);
            result.append(process);
            result.append(">");
            if (arguments.size() > 1) {
                DataTypeParameter dataTypeParameter = arguments.get(1);
                result.append("(");
                result.append(process(dataTypeParameter));
                result.append(")");
            }
        }
        return result.toString();
    }

    @Override
    public String visitAdbMysqlPointDataType(AdbMysqlPointDataType adbMysqlPointDataType, Void context) {
        StringBuilder builder = new StringBuilder();
        builder.append("POINT");
        if (adbMysqlPointDataType.getDelimiterTokenizer() != null) {
            builder.append(" DELIMITER_TOKENIZER ");
            builder.append(formatStringLiteral(adbMysqlPointDataType.getDelimiterTokenizer().getValue()));
        }
        return builder.toString();
    }

    //lambdaParameter LAMBDA_IMPLEMENT expressionAtom
    @Override
    public String visitLambdaExpression(LambdaExpression lambdaExpression, Void context) {
        StringBuilder stringBuilder = new StringBuilder();
        Identifier identifier = lambdaExpression.getIdentifier();
        String id = process(identifier);
        stringBuilder.append(id);
        stringBuilder.append("->");
        BaseExpression expression = lambdaExpression.getExpression();
        String process = process(expression);
        stringBuilder.append(process);
        return stringBuilder.toString();
    }
}
