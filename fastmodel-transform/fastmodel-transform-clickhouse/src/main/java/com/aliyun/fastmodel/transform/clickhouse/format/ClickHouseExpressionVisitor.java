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

package com.aliyun.fastmodel.transform.clickhouse.format;

import java.math.BigDecimal;
import java.util.List;

import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.DoubleLiteral;
import com.aliyun.fastmodel.transform.api.format.DefaultExpressionVisitor;
import com.aliyun.fastmodel.transform.clickhouse.context.ClickHouseContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.tree.ClickHouseEnumDataTypeParameter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * clickHouse的表达式格式化
 *
 * @author panguanjing
 */
public class ClickHouseExpressionVisitor extends DefaultExpressionVisitor {

    private final ClickHouseContext clickHouseContext;

    public ClickHouseExpressionVisitor(ClickHouseContext clickHouseContext) {
        this.clickHouseContext = clickHouseContext;
    }

    @Override
    public String visitIdentifier(Identifier node, Void context) {
        String value = StringUtils.isNotBlank(node.getOrigin()) ? StripUtils.strip(node.getOrigin()) : node.getValue();
        if (node.isDelimited()) {
            return StripUtils.addPrefix(StripUtils.strip(value));
        }
        return value;
    }

    /**
     * ClickHouse 的字符串使用单引号,内部单引号与反斜杠通过反斜杠转义
     *
     * @param s 字符串内容
     * @return 转义后的字符串字面量
     */
    @Override
    public String formatStringLiteral(String s) {
        if (s == null) {
            return "''";
        }
        String result = s.replace("\\", "\\\\");
        result = result.replace("'", "\\'");
        return "'" + result + "'";
    }

    /**
     * 浮点字面量使用普通十进制输出,避免出现 0E0 这类的科学计数形态
     *
     * @param doubleLiteral 浮点字面量
     * @param context 上下文
     * @return 十进制字面量
     */
    @Override
    public String visitDoubleLiteral(DoubleLiteral doubleLiteral, Void context) {
        if (doubleLiteral.getValue() == null) {
            return null;
        }
        return BigDecimal.valueOf(doubleLiteral.getValue()).stripTrailingZeros().toPlainString();
    }

    /**
     * ClickHouse 的类型参数形态较多，Enum8('a' = 1) 的参数是"名称 = 枚举值"，
     * 需要单独拼装，其余参数(NumericParameter/TypeParameter)沿用通用处理
     *
     * @param genericDataType 数据类型
     * @param context 上下文
     * @return 类型文本
     */
    @Override
    public String visitGenericDataType(GenericDataType genericDataType, Void context) {
        IDataTypeName typeName = genericDataType.getTypeName();
        String typeValue = typeName == null ? null : typeName.getValue();
        List<DataTypeParameter> arguments = genericDataType.getArguments();
        if (CollectionUtils.isEmpty(arguments)) {
            return typeValue;
        }
        StringBuilder stringBuilder = new StringBuilder(typeValue).append("(");
        for (int i = 0; i < arguments.size(); i++) {
            if (i > 0) {
                stringBuilder.append(",");
            }
            stringBuilder.append(formatDataTypeParameter(arguments.get(i)));
        }
        return stringBuilder.append(")").toString();
    }

    private String formatDataTypeParameter(DataTypeParameter dataTypeParameter) {
        if (!(dataTypeParameter instanceof ClickHouseEnumDataTypeParameter)) {
            return process(dataTypeParameter);
        }
        ClickHouseEnumDataTypeParameter enumParameter = (ClickHouseEnumDataTypeParameter)dataTypeParameter;
        return process(enumParameter.getName()) + " = " + process(enumParameter.getBaseLiteral());
    }
}
